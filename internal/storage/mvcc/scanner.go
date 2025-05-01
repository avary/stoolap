package mvcc

import (
	"sync"

	"github.com/semihalev/stoolap/internal/storage"
	"github.com/semihalev/stoolap/internal/storage/expression"
)

// MVCCScannerMode defines the scanner's operation mode
type MVCCScannerMode int

// MVCCScanner is a simple scanner implementation for MVCC results
type MVCCScanner struct {
	sourceRows    map[int64]storage.Row             // Original row references, not copied
	rowIDs        []int64                           // Sorted row IDs for iteration
	currentIndex  int                               // Current position in rowIDs
	columnIndices []int                             // Which columns to return
	schema        storage.Schema                    // Schema information
	err           error                             // Any error that occurred
	rowMap        map[int64]storage.Row             // The original map, will be returned to pool on Close
	whereExpr     *expression.SchemaAwareExpression // The schema-aware expression, if it needs to be returned to pool
	projectedRow  storage.Row                       // Reusable buffer for column projection
}

// Next advances to the next row
func (s *MVCCScanner) Next() bool {
	// Check for errors
	if s.err != nil {
		return false
	}

	// Advance to next row
	s.currentIndex++

	// Check if we've reached the end
	return s.currentIndex < len(s.rowIDs)
}

// Row returns the current row, with projection if needed
func (s *MVCCScanner) Row() storage.Row {
	if s.currentIndex < 0 || s.currentIndex >= len(s.rowIDs) {
		return nil
	}

	// Row mode implementation (the original logic)
	// Get the current row
	rowID := s.rowIDs[s.currentIndex]
	row := s.sourceRows[rowID]

	// If no column projection is required, return the row directly
	if len(s.columnIndices) == 0 {
		return row
	}

	// Fill the projection buffer with values from the source row
	for i, colIdx := range s.columnIndices {
		if colIdx < len(row) {
			s.projectedRow[i] = row[colIdx]
		} else {
			s.projectedRow[i] = nil // Handle out-of-bounds gracefully
		}
	}

	return s.projectedRow
}

// Err returns any error that occurred during scanning
func (s *MVCCScanner) Err() error {
	return s.err
}

// Close releases any resources held by the scanner
func (s *MVCCScanner) Close() error {
	clear(s.projectedRow)
	s.projectedRow = s.projectedRow[:0]

	// Return the row map to the pool if it came from there
	if s.rowMap != nil {
		PutRowMap(s.rowMap)
		s.rowMap = nil
	}

	// Return the schema aware expression to pool if applicable
	if s.whereExpr != nil {
		expression.ReturnSchemaAwereExpressionPool(s.whereExpr)
		s.whereExpr = nil
	}

	// Return the scanner itself to the pool
	ReturnMVCCScanner(s)

	return nil
}

// Pool for rowIDs slices to reduce allocations
var rowIDsPool = sync.Pool{
	New: func() interface{} {
		// Default initial capacity for rowIDs
		sp := make([]int64, 0, 128)
		return &sp
	},
}

// GetRowIDSlice gets a slice from the pool with appropriate capacity
func GetRowIDSlice(capacity int) []int64 {
	sliceInterface := rowIDsPool.Get()
	sp, ok := sliceInterface.(*[]int64)
	if !ok || sp == nil {
		return make([]int64, 0, capacity)
	}
	slice := *sp
	// Clear the slice but preserve capacity
	slice = slice[:0]

	// If existing capacity is too small, create a new slice
	if cap(slice) < capacity {
		return make([]int64, 0, capacity)
	}

	return slice
}

// PutRowIDSlice returns a slice to the pool
func PutRowIDSlice(slice []int64) {
	if slice == nil || cap(slice) == 0 {
		return
	}

	// Clear the slice and return to pool
	slice = slice[:0]
	rowIDsPool.Put(&slice)
}

// RangeScanner is a specialized scanner for efficient ID range queries
type RangeScanner struct {
	txnID         int64                 // Transaction ID for visibility checks
	versionStore  *VersionStore         // Access to versions
	currentID     int64                 // Current ID in range
	endID         int64                 // End ID in range (inclusive)
	columnIndices []int                 // Columns to include
	schema        storage.Schema        // Schema information
	err           error                 // Any scanning errors
	projectedRow  storage.Row           // Reused buffer for projection
	currentRow    storage.Row           // Current row being processed
	batchSize     int                   // Size of batches for prefetching
	currentBatch  map[int64]*RowVersion // Current batch of prefetched rows
	inclusive     bool                  // Whether endID is inclusive
}

// NewRangeScanner creates a scanner optimized for ID range scans
// This implementation works efficiently with consecutive ID ranges
func NewRangeScanner(
	versionStore *VersionStore,
	startID, endID int64,
	inclusive bool,
	txnID int64,
	schema storage.Schema,
	columnIndices []int,
) *RangeScanner {
	return &RangeScanner{
		txnID:         txnID,
		versionStore:  versionStore,
		currentID:     startID,
		endID:         endID,
		inclusive:     inclusive,
		columnIndices: columnIndices,
		schema:        schema,
		batchSize:     1000, // Batch size for prefetching
		currentBatch:  make(map[int64]*RowVersion, 1000),
		projectedRow:  make(storage.Row, len(columnIndices)),
	}
}

// Next advances to the next row in the range
func (s *RangeScanner) Next() bool {
	// Check for errors
	if s.err != nil {
		return false
	}

	// Calculate actual end condition based on inclusive flag
	actualEnd := s.endID
	if !s.inclusive {
		actualEnd = s.endID - 1
	}

	// Check if we've reached the end of the range
	if s.currentID > actualEnd {
		return false
	}

	// Fetch the next visible row
	for s.currentID <= actualEnd {
		// Check if we need to fetch a new batch
		if len(s.currentBatch) == 0 {
			// Prepare batch of IDs to fetch
			endBatch := s.currentID + int64(s.batchSize) - 1
			if endBatch > actualEnd {
				endBatch = actualEnd
			}

			// Prepare ID slice for batch
			ids := make([]int64, 0, endBatch-s.currentID+1)
			for id := s.currentID; id <= endBatch; id++ {
				ids = append(ids, id)
			}

			// Fetch batch of versions
			s.currentBatch = s.versionStore.GetVisibleVersionsByIDs(ids, s.txnID)

			// If batch is empty, we can skip to the next batch
			if len(s.currentBatch) == 0 {
				s.currentID = endBatch + 1
				continue
			}
		}

		// Check if the current ID exists and is visible
		if version, exists := s.currentBatch[s.currentID]; exists && !version.IsDeleted {
			s.currentRow = version.Data
			s.currentID++ // Advance ID for next iteration
			return true
		}

		// ID doesn't exist or is deleted, try next ID
		delete(s.currentBatch, s.currentID)
		s.currentID++

		// If batch is empty after this deletion, we'll fetch a new batch next time
	}

	return false
}

// Row returns the current row, with projection if needed
func (s *RangeScanner) Row() storage.Row {
	if s.currentRow == nil {
		return nil
	}

	// If no column projection is required, return the row directly
	if len(s.columnIndices) == 0 {
		return s.currentRow
	}

	// Fill the projection buffer with values from the source row
	for i, colIdx := range s.columnIndices {
		if colIdx < len(s.currentRow) {
			s.projectedRow[i] = s.currentRow[colIdx]
		} else {
			s.projectedRow[i] = nil // Handle out-of-bounds gracefully
		}
	}

	return s.projectedRow
}

// Err returns any error that occurred during scanning
func (s *RangeScanner) Err() error {
	return s.err
}

// Close releases resources
func (s *RangeScanner) Close() error {
	clear(s.projectedRow)
	s.projectedRow = s.projectedRow[:0]

	// Return the version map to the pool
	if s.currentBatch != nil {
		ReturnVisibleVersionMap(s.currentBatch)
		s.currentBatch = nil
	}

	// Clear references
	s.currentRow = nil
	s.versionStore = nil

	return nil
}

// Pool for MVCCScanner instances to reduce allocations
var mvccScannerPool = sync.Pool{
	New: func() interface{} {
		return &MVCCScanner{
			currentIndex: -1,
		}
	},
}

// GetMVCCScanner gets a scanner from the pool
func GetMVCCScanner() *MVCCScanner {
	return mvccScannerPool.Get().(*MVCCScanner)
}

// ReturnMVCCScanner returns a scanner to the pool
func ReturnMVCCScanner(scanner *MVCCScanner) {
	if scanner == nil {
		return
	}

	// Return the rowIDs slice to the pool
	if scanner.rowIDs != nil {
		PutRowIDSlice(scanner.rowIDs)
	}

	// Return the scanner to the pool after cleaning up references
	scanner.sourceRows = nil
	scanner.rowIDs = nil
	scanner.columnIndices = nil
	// We can't set schema to nil as it's a value type, not a pointer
	scanner.projectedRow = nil
	scanner.rowMap = nil
	scanner.whereExpr = nil
	scanner.err = nil
	scanner.currentIndex = -1

	mvccScannerPool.Put(scanner)
}

// NewMVCCScanner creates a new scanner for MVCC-tracked rows
// Memory-optimized version that doesn't copy rows unnecessarily
func NewMVCCScanner(rows map[int64]storage.Row, schema storage.Schema, columnIndices []int, where storage.Expression) *MVCCScanner {
	// Get a scanner from the pool instead of creating a new one
	scanner := GetMVCCScanner()

	// Initialize reused scanner fields
	scanner.sourceRows = rows                 // Use the rows map directly to avoid copying
	scanner.rowIDs = GetRowIDSlice(len(rows)) // Get a reused slice from the pool
	scanner.currentIndex = -1
	scanner.columnIndices = columnIndices
	scanner.schema = schema
	scanner.rowMap = rows // Store the original map for returning to the pool

	// Store the expression if it's a SchemaAwareExpression for cleanup
	if saExpr, ok := where.(*expression.SchemaAwareExpression); ok {
		scanner.whereExpr = saExpr
	}

	scanner.projectedRow = make(storage.Row, len(columnIndices))

	// Filtering phase - apply where expression
	for rowID, row := range rows {
		// Skip rows that don't match the filter
		if where != nil {
			matches, err := where.Evaluate(row)
			if err != nil {
				scanner.err = err
				return scanner
			}
			if !matches {
				continue
			}
		}

		// Add the matching row ID to the list
		scanner.rowIDs = append(scanner.rowIDs, rowID)
	}

	// For most database operations, we want rows sorted by ID
	// Use our SIMD-optimized sorting algorithm for int64 slices
	if len(scanner.rowIDs) > 0 {
		SIMDSortInt64s(scanner.rowIDs)
	}

	return scanner
}
