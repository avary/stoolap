// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// Ordering function type for comparing multi-column values
type multiCompareFunc func(a, b []storage.ColumnValue) int

// MultiColumnNode represents a node in the multi-column B-tree
type multiColumnNode struct {
	keys     [][]storage.ColumnValue // Multi-column keys stored in this node
	rowIDs   [][]int64               // Row IDs for each key
	children []*multiColumnNode      // Child nodes (nil for leaf nodes)
	isLeaf   bool                    // Whether this is a leaf node
	compare  multiCompareFunc        // Function for comparing keys
}

// findIndex returns the index where the key should be inserted
func (n *multiColumnNode) findIndex(key []storage.ColumnValue) int {
	// Binary search for the key
	return sort.Search(len(n.keys), func(i int) bool {
		return n.compare(n.keys[i], key) >= 0
	})
}

// insert inserts a key and rowID into the B-tree node
func (n *multiColumnNode) insert(key []storage.ColumnValue, rowID int64) {
	i := n.findIndex(key)

	// If key already exists, just append the rowID
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		// Check if rowID already exists to avoid duplicates
		for _, id := range n.rowIDs[i] {
			if id == rowID {
				return // rowID already exists, nothing to do
			}
		}

		// Add rowID in sorted order
		j := sort.Search(len(n.rowIDs[i]), func(j int) bool {
			return n.rowIDs[i][j] >= rowID
		})

		// Insert at position j
		if j == len(n.rowIDs[i]) {
			// Append to end
			n.rowIDs[i] = append(n.rowIDs[i], rowID)
		} else {
			// Insert at position j
			n.rowIDs[i] = append(n.rowIDs[i][:j+1], n.rowIDs[i][j:]...)
			n.rowIDs[i][j] = rowID
		}
		return
	}

	// Insert new key and rowID
	n.keys = append(n.keys, nil)
	n.rowIDs = append(n.rowIDs, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.rowIDs[i+1:], n.rowIDs[i:])

	// Create a deep copy of the key to ensure it won't be modified
	keyCopy := make([]storage.ColumnValue, len(key))
	copy(keyCopy, key)

	n.keys[i] = keyCopy
	n.rowIDs[i] = []int64{rowID}
}

// remove removes a rowID from a key in the B-tree node
func (n *multiColumnNode) remove(key []storage.ColumnValue, rowID int64) bool {
	i := n.findIndex(key)
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		// Find and remove the rowID
		for j, id := range n.rowIDs[i] {
			if id == rowID {
				// Remove by swapping with last element and truncating
				n.rowIDs[i][j] = n.rowIDs[i][len(n.rowIDs[i])-1]
				n.rowIDs[i] = n.rowIDs[i][:len(n.rowIDs[i])-1]

				// If no more rowIDs for this key, remove the key
				if len(n.rowIDs[i]) == 0 {
					copy(n.keys[i:], n.keys[i+1:])
					copy(n.rowIDs[i:], n.rowIDs[i+1:])
					n.keys = n.keys[:len(n.keys)-1]
					n.rowIDs = n.rowIDs[:len(n.rowIDs)-1]
				}
				return true
			}
		}
	}
	return false
}

// findLeftBoundary finds the left (starting) boundary of a range search
func (n *multiColumnNode) findLeftBoundary(min []storage.ColumnValue, includeMin bool) int {
	// Handle NULL or empty min case
	if len(min) == 0 {
		return 0 // Include all from beginning
	}

	// Use binary search
	left, right := 0, len(n.keys)-1

	// Special case for tiny arrays: linear scan might be faster
	if right < 8 {
		for i := 0; i <= right; i++ {
			compare := n.compare(n.keys[i], min)
			if includeMin && compare >= 0 {
				return i
			} else if !includeMin && compare > 0 {
				return i
			}
		}
		return right + 1
	}

	// Standard binary search with custom comparison function
	for left <= right {
		mid := left + (right-left)/2
		compare := n.compare(n.keys[mid], min)

		if includeMin {
			// Looking for first key >= min
			if compare < 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		} else {
			// Looking for first key > min
			if compare <= 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		}
	}

	// Left is now the index of the first key that should be included
	return left
}

// findRightBoundary finds the right (ending) boundary of a range search
func (n *multiColumnNode) findRightBoundary(max []storage.ColumnValue, includeMax bool) int {
	// Handle NULL or empty max case
	if len(max) == 0 {
		return len(n.keys) // Include all to the end
	}

	// Use binary search
	left, right := 0, len(n.keys)-1

	// Special case for tiny arrays: linear scan might be faster
	if right < 8 {
		for i := 0; i <= right; i++ {
			compare := n.compare(n.keys[i], max)
			if includeMax && compare > 0 {
				return i
			} else if !includeMax && compare >= 0 {
				return i
			}
		}
		return right + 1
	}

	// Standard binary search with custom comparison function
	for left <= right {
		mid := left + (right-left)/2
		compare := n.compare(n.keys[mid], max)

		if includeMax {
			// Looking for first key > max
			if compare <= 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		} else {
			// Looking for first key >= max
			if compare < 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		}
	}

	// Left is now the index of the first key that should be excluded
	return left
}

// rangeSearch finds all rowIDs in a range [min, max]
func (n *multiColumnNode) rangeSearch(min, max []storage.ColumnValue, includeMin, includeMax bool, result *[]int64) {
	if len(n.keys) == 0 {
		return
	}

	// Get the start and end boundaries using optimized functions
	startIdx := n.findLeftBoundary(min, includeMin)
	endIdx := n.findRightBoundary(max, includeMax)

	// Early return if no overlap
	if startIdx >= len(n.keys) || endIdx <= 0 || startIdx >= endIdx {
		return
	}

	// Fast path: If start and end are close, we can estimate the number of IDs
	if endIdx-startIdx < 64 { // Small range optimization
		// Estimate result capacity to reduce reallocations
		estimatedCapacity := 0
		for i := startIdx; i < endIdx; i++ {
			estimatedCapacity += len(n.rowIDs[i])
		}

		// Pre-allocate result capacity if needed
		if estimatedCapacity > 0 && cap(*result)-len(*result) < estimatedCapacity {
			newResult := make([]int64, len(*result), len(*result)+estimatedCapacity)
			copy(newResult, *result)
			*result = newResult
		}
	}

	// Collect all rowIDs in the range
	for i := startIdx; i < endIdx; i++ {
		// Direct append for the common case of 1 or few row IDs per key
		if len(n.rowIDs[i]) == 1 {
			*result = append(*result, n.rowIDs[i][0])
		} else {
			*result = append(*result, n.rowIDs[i]...)
		}
	}
}

// equalSearch finds all rowIDs with the given key
func (n *multiColumnNode) equalSearch(key []storage.ColumnValue, result *[]int64) {
	i := n.findIndex(key)
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		// For single match optimization
		if len(n.rowIDs[i]) == 1 {
			*result = append(*result, n.rowIDs[i][0])
			return
		}
		*result = append(*result, n.rowIDs[i]...)
	}
}

// getAll returns all rowIDs in the node
func (n *multiColumnNode) getAll(result *[]int64) {
	for _, ids := range n.rowIDs {
		*result = append(*result, ids...)
	}
}

// multiColumnTree implements a B-tree data structure optimized for multi-column indexes
type multiColumnTree struct {
	root    *multiColumnNode // Root node of the B-tree
	compare multiCompareFunc // Function for comparing keys
	size    int              // Number of keys in the tree
}

// newMultiColumnTree creates a new B-tree with the given compare function
func newMultiColumnTree(compare multiCompareFunc) *multiColumnTree {
	return &multiColumnTree{
		root: &multiColumnNode{
			keys:     make([][]storage.ColumnValue, 0),
			rowIDs:   make([][]int64, 0),
			children: nil,
			isLeaf:   true,
			compare:  compare,
		},
		compare: compare,
		size:    0,
	}
}

// Insert adds a key and rowID to the B-tree
func (t *multiColumnTree) Insert(key []storage.ColumnValue, rowID int64) {
	t.root.insert(key, rowID)
	t.size++
}

// Remove removes a key and rowID from the B-tree
func (t *multiColumnTree) Remove(key []storage.ColumnValue, rowID int64) bool {
	result := t.root.remove(key, rowID)
	if result {
		t.size--
	}
	return result
}

// ValueCount returns the number of occurrences of a key
func (t *multiColumnTree) ValueCount(key []storage.ColumnValue) int {
	i := t.root.findIndex(key)
	if i < len(t.root.keys) && t.compare(t.root.keys[i], key) == 0 {
		return len(t.root.rowIDs[i])
	}
	return 0
}

// RangeSearch finds all rowIDs in a range [min, max]
func (t *multiColumnTree) RangeSearch(min, max []storage.ColumnValue, includeMin, includeMax bool) []int64 {
	// Estimate capacity based on range size and tree characteristics
	estimatedCapacity := t.estimateRangeSize(min, max)
	result := make([]int64, 0, estimatedCapacity)

	// Perform the range search
	t.root.rangeSearch(min, max, includeMin, includeMax, &result)

	// If result is much smaller than capacity, consider trimming
	if len(result) > 0 && cap(result) > 2*len(result) && cap(result) > 1000 {
		trimmedResult := make([]int64, len(result))
		copy(trimmedResult, result)
		return trimmedResult
	}

	return result
}

// estimateRangeSize estimates the number of rows in a given range
func (t *multiColumnTree) estimateRangeSize(min, max []storage.ColumnValue) int {
	// Default reasonable capacity
	defaultCapacity := 100

	// If tree is empty, return minimum capacity
	if t.size == 0 || t.root == nil || len(t.root.keys) == 0 {
		return defaultCapacity
	}

	// For unbounded ranges (no min or max), use a proportion of the tree size
	if min == nil && max == nil {
		return t.size
	}

	// Try to get a rough range size estimate
	if min != nil && max != nil {
		// Find the start and end boundaries
		startIdx := t.root.findLeftBoundary(min, true)
		endIdx := t.root.findRightBoundary(max, true)

		// If we can determine the boundaries
		if startIdx < len(t.root.keys) && endIdx <= len(t.root.keys) && startIdx < endIdx {
			// Count the actual number of rowIDs in this range
			estimateCap := 0
			for i := startIdx; i < endIdx && i < len(t.root.keys); i++ {
				estimateCap += len(t.root.rowIDs[i])
			}

			// Use actual count with some buffer
			return estimateCap + 10
		}

		// If we have some range information but can't determine exact count
		range_ratio := float64(endIdx-startIdx) / float64(len(t.root.keys))
		if range_ratio > 0 && range_ratio <= 1.0 {
			return int(float64(t.size)*range_ratio) + 10
		}
	}

	// If we have only min or only max, use half the tree size as an estimate
	if (min != nil && max == nil) || (min == nil && max != nil) {
		return t.size/2 + 10
	}

	// Fallback to a reasonable default
	return defaultCapacity
}

// EqualSearch finds all rowIDs with the given key
func (t *multiColumnTree) EqualSearch(key []storage.ColumnValue) []int64 {
	result := make([]int64, 0, 1) // Start with a reasonable capacity
	t.root.equalSearch(key, &result)
	return result
}

// GetAll returns all rowIDs in the B-tree
func (t *multiColumnTree) GetAll() []int64 {
	result := make([]int64, 0, t.size)
	t.root.getAll(&result)
	return result
}

// Size returns the number of keys in the B-tree
func (t *multiColumnTree) Size() int {
	return t.size
}

// Clear removes all entries from the B-tree
func (t *multiColumnTree) Clear() {
	t.root = &multiColumnNode{
		keys:     make([][]storage.ColumnValue, 0),
		rowIDs:   make([][]int64, 0),
		children: nil,
		isLeaf:   true,
		compare:  t.compare,
	}
	t.size = 0
}

// compareMultiColumnValues compares two multi-column key slices
func compareMultiColumnValues(a, b []storage.ColumnValue) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Compare each column in order
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		// Handle NULL values in comparison
		aIsNull := a[i] == nil || a[i].IsNull()
		bIsNull := b[i] == nil || b[i].IsNull()

		if aIsNull && bIsNull {
			continue // Both are NULL, compare next column
		}
		if aIsNull {
			return -1 // NULL sorts before non-NULL
		}
		if bIsNull {
			return 1 // non-NULL sorts after NULL
		}

		cmp, err := a[i].Compare(b[i])
		if err != nil {
			// Error handling - in production code would log this
			continue
		}

		if cmp != 0 {
			return cmp
		}
	}

	// If all common columns are equal, shorter array comes first
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}

	return 0 // Completely equal
}

// MultiColumnarIndex represents an improved multi-column index implementation
// using a B-tree data structure for efficient lookups and NULL tracking per column
type MultiColumnarIndex struct {
	name         string
	tableName    string
	columnNames  []string
	columnIDs    []int
	dataTypes    []storage.DataType
	isUnique     bool
	versionStore *VersionStore

	// B-tree for values
	valueTree *multiColumnTree

	// nullRowsByColumn tracks NULL rows per column - key is columnID
	nullRowsByColumn map[int][]int64

	// mutex for thread-safety
	mutex sync.RWMutex
}

// NewMultiColumnarIndex creates a new MultiColumnarIndex
func NewMultiColumnarIndex(
	name string,
	tableName string,
	columnNames []string,
	columnIDs []int,
	dataTypes []storage.DataType,
	versionStore *VersionStore,
	isUnique bool) *MultiColumnarIndex {

	idx := &MultiColumnarIndex{
		name:             name,
		tableName:        tableName,
		columnNames:      columnNames,
		columnIDs:        columnIDs,
		dataTypes:        dataTypes,
		valueTree:        newMultiColumnTree(compareMultiColumnValues),
		nullRowsByColumn: make(map[int][]int64),
		versionStore:     versionStore,
		isUnique:         isUnique,
	}

	// Initialize nullRowsByColumn entries for each column
	for _, colID := range columnIDs {
		idx.nullRowsByColumn[colID] = make([]int64, 0, 16)
	}

	return idx
}

// Storage.Index interface implementation

// Name returns the index name
func (idx *MultiColumnarIndex) Name() string {
	return idx.name
}

// TableName returns the table this index belongs to
func (idx *MultiColumnarIndex) TableName() string {
	return idx.tableName
}

// ColumnNames returns the names of indexed columns
func (idx *MultiColumnarIndex) ColumnNames() []string {
	return idx.columnNames
}

// ColumnIDs returns the IDs of indexed columns
func (idx *MultiColumnarIndex) ColumnIDs() []int {
	return idx.columnIDs
}

// DataTypes returns the data types of indexed columns
func (idx *MultiColumnarIndex) DataTypes() []storage.DataType {
	return idx.dataTypes
}

// IndexType returns the type of the index
func (idx *MultiColumnarIndex) IndexType() storage.IndexType {
	return storage.ColumnarIndex
}

// IsUnique returns whether this is a unique index
func (idx *MultiColumnarIndex) IsUnique() bool {
	return idx.isUnique
}

// Add adds values to the index
func (idx *MultiColumnarIndex) Add(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Track NULL values per column
	hasAnyNull := false
	for i, val := range values {
		if val == nil || val.IsNull() {
			hasAnyNull = true
			colID := idx.columnIDs[i]

			// Check if rowID already exists
			exists := false
			for _, id := range idx.nullRowsByColumn[colID] {
				if id == rowID {
					exists = true
					break
				}
			}

			if !exists {
				idx.nullRowsByColumn[colID] = append(idx.nullRowsByColumn[colID], rowID)
			}
		}
	}

	// If any value is NULL, we don't add the row to the tree
	// This is because NULL values are handled separately
	if hasAnyNull {
		return nil
	}

	// For unique indexes, check if the value combination already exists
	if idx.isUnique {
		count := idx.valueTree.ValueCount(values)
		if count > 0 {
			return storage.NewUniqueConstraintError(
				idx.name,
				strings.Join(idx.columnNames, ","),
				values[0],
			)
		}
	}

	// Insert the values into the tree
	idx.valueTree.Insert(values, rowID)
	return nil
}

// AddBatch adds multiple entries to the index in a single batch operation
// This is more efficient than multiple individual Add operations
func (idx *MultiColumnarIndex) AddBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// For unique indexes, we need to check constraints first
	if idx.isUnique {
		// First check all non-NULL value combinations for uniqueness
		for _, values := range entries {
			// Validate column count
			if len(values) != len(idx.columnIDs) {
				return fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
			}

			// Skip entries with NULL values
			hasNull := false
			for _, val := range values {
				if val == nil || val.IsNull() {
					hasNull = true
					break
				}
			}

			if !hasNull {
				// Check uniqueness for full value set
				count := idx.valueTree.ValueCount(values)
				if count > 0 {
					// Return unique constraint violation
					return storage.NewUniqueConstraintError(
						idx.name,
						strings.Join(idx.columnNames, ","),
						values[0],
					)
				}
			}
		}
	}

	// Now process all entries
	for rowID, values := range entries {
		// Validate column count
		if len(values) != len(idx.columnIDs) {
			return fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
		}

		// Track NULL values per column
		hasAnyNull := false
		for i, val := range values {
			if val == nil || val.IsNull() {
				hasAnyNull = true
				colID := idx.columnIDs[i]

				// Check if rowID already exists
				exists := false
				for _, id := range idx.nullRowsByColumn[colID] {
					if id == rowID {
						exists = true
						break
					}
				}

				if !exists {
					idx.nullRowsByColumn[colID] = append(idx.nullRowsByColumn[colID], rowID)
				}
			}
		}

		// If no NULL values, add to the tree
		if !hasAnyNull {
			idx.valueTree.Insert(values, rowID)
		}
	}

	return nil
}

// Remove removes values from the index
func (idx *MultiColumnarIndex) Remove(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Track NULL values per column
	hasAnyNull := false
	for i, val := range values {
		if val == nil || val.IsNull() {
			hasAnyNull = true
			colID := idx.columnIDs[i]

			// Find and remove rowID from nullRowsByColumn for this column
			for j, id := range idx.nullRowsByColumn[colID] {
				if id == rowID {
					// Remove by swapping with the last element and truncating
					lastIdx := len(idx.nullRowsByColumn[colID]) - 1
					idx.nullRowsByColumn[colID][j] = idx.nullRowsByColumn[colID][lastIdx]
					idx.nullRowsByColumn[colID] = idx.nullRowsByColumn[colID][:lastIdx]
					break
				}
			}
		}
	}

	// If any value is NULL, we don't need to remove from the tree
	// because the row wasn't added to the tree in the first place
	if hasAnyNull {
		return nil
	}

	// Remove from the tree
	idx.valueTree.Remove(values, rowID)
	return nil
}

// RemoveBatch removes multiple entries from the index in a single batch operation
// This is more efficient than multiple individual Remove operations
func (idx *MultiColumnarIndex) RemoveBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Process all entries
	for rowID, values := range entries {
		// Validate column count
		if len(values) != len(idx.columnIDs) {
			return fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
		}

		// Track NULL values per column
		hasAnyNull := false
		for i, val := range values {
			if val == nil || val.IsNull() {
				hasAnyNull = true
				colID := idx.columnIDs[i]

				// Find and remove rowID from nullRowsByColumn for this column
				for j, id := range idx.nullRowsByColumn[colID] {
					if id == rowID {
						// Remove by swapping with the last element and truncating
						lastIdx := len(idx.nullRowsByColumn[colID]) - 1
						idx.nullRowsByColumn[colID][j] = idx.nullRowsByColumn[colID][lastIdx]
						idx.nullRowsByColumn[colID] = idx.nullRowsByColumn[colID][:lastIdx]
						break
					}
				}
			}
		}

		// If any value is NULL, we don't need to remove from the tree
		// because the row wasn't added to the tree in the first place
		if !hasAnyNull {
			// Remove from the tree
			idx.valueTree.Remove(values, rowID)
		}
	}

	return nil
}

func (idx *MultiColumnarIndex) Find(values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return nil, fmt.Errorf("expected %d values for multi-column index, got %d", len(idx.columnIDs), len(values))
	}

	// Get matching row IDs
	rowIDs := idx.GetRowIDsEqual(values)
	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// GetRowIDsEqual returns row IDs with the given values
func (idx *MultiColumnarIndex) GetRowIDsEqual(values []storage.ColumnValue) []int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Special case for nil values - return rows with NULL in first column
	if values == nil {
		colID := idx.columnIDs[0]
		if len(idx.nullRowsByColumn[colID]) == 0 {
			return nil
		}
		result := make([]int64, len(idx.nullRowsByColumn[colID]))
		copy(result, idx.nullRowsByColumn[colID])
		return result
	}

	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return []int64{}
	}

	// Check for NULL values in any column
	for i, val := range values {
		if val == nil || val.IsNull() {
			colID := idx.columnIDs[i]

			// If NULL in this column is requested, return all rows with NULL in this column
			if len(idx.nullRowsByColumn[colID]) == 0 {
				return nil
			}

			// Return a copy of nullRows for this column
			result := make([]int64, len(idx.nullRowsByColumn[colID]))
			copy(result, idx.nullRowsByColumn[colID])
			return result
		}
	}

	// Use the tree to find matching rows
	return idx.valueTree.EqualSearch(values)
}

// FindRange finds all row IDs where the values are in the given range
func (idx *MultiColumnarIndex) FindRange(min, max []storage.ColumnValue,
	minInclusive, maxInclusive bool) ([]storage.IndexEntry, error) {

	// Get row IDs in the range
	rowIDs := idx.GetRowIDsInRange(min, max, minInclusive, maxInclusive)
	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// GetRowIDsInRange returns row IDs with values in the given range
func (idx *MultiColumnarIndex) GetRowIDsInRange(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	// Get the tree's lock to ensure consistent reads
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Case 1: Single column range scan on first column (most common case)
	if len(minValues) == 1 && len(maxValues) == 1 && len(idx.columnIDs) > 0 {
		// For a single column range query on the first column, we need to return all rows
		// that have the first column value in the specified range, regardless of other columns

		// Create a normalized version with the full idx.columnIDs array and
		// put the range values in the first position
		min := make([]storage.ColumnValue, len(idx.columnIDs))
		max := make([]storage.ColumnValue, len(idx.columnIDs))

		// Setting the first column, all other columns are nil (unbounded)
		min[0] = minValues[0]
		max[0] = maxValues[0]

		// First get all matching rows with only the first column bounded
		allRowIDs := make([]int64, 0)

		// Go through all keys in the tree
		for keyIndex, key := range idx.valueTree.root.keys {
			// Skip keys with nil or empty first column
			if len(key) == 0 || key[0] == nil {
				continue
			}

			// Check if this key's first column is in the specified range
			inRange := true

			// Check minimum bound
			if min[0] != nil {
				cmp, err := key[0].Compare(min[0])
				if err != nil || (includeMin && cmp < 0) || (!includeMin && cmp <= 0) {
					inRange = false
				}
			}

			// Check maximum bound if still in range
			if inRange && max[0] != nil {
				cmp, err := key[0].Compare(max[0])
				if err != nil || (includeMax && cmp > 0) || (!includeMax && cmp >= 0) {
					inRange = false
				}
			}

			// If this key's first column is in range, add all its row IDs
			if inRange {
				allRowIDs = append(allRowIDs, idx.valueTree.root.rowIDs[keyIndex]...)
			}
		}

		return allRowIDs
	}

	// Handle special case for single-column index (optimize for common operation)
	if len(idx.columnIDs) == 1 && (minValues != nil || maxValues != nil) {
		allRowIDs := make([]int64, 0)

		// Go through all keys in the tree
		for keyIndex, key := range idx.valueTree.root.keys {
			// Skip keys with nil or empty first column
			if len(key) == 0 || key[0] == nil {
				continue
			}

			// Check if this key's first column is in the specified range
			inRange := true

			// Check minimum bound
			if len(minValues) > 0 && minValues[0] != nil {
				cmp, err := key[0].Compare(minValues[0])
				if err != nil || (includeMin && cmp < 0) || (!includeMin && cmp <= 0) {
					inRange = false
				}
			}

			// Check maximum bound if still in range
			if inRange && maxValues != nil && len(maxValues) > 0 && maxValues[0] != nil {
				cmp, err := key[0].Compare(maxValues[0])
				if err != nil || (includeMax && cmp > 0) || (!includeMax && cmp >= 0) {
					inRange = false
				}
			}

			// If this key's first column is in range, add all its row IDs
			if inRange {
				allRowIDs = append(allRowIDs, idx.valueTree.root.rowIDs[keyIndex]...)
			}
		}

		return allRowIDs
	}

	// Case 2: Multi-column range query
	// For composite range queries, we need to check that each column's value
	// falls within its specified range bounds

	// Normalize min/max values to match the number of columns
	min := make([]storage.ColumnValue, len(idx.columnIDs))
	max := make([]storage.ColumnValue, len(idx.columnIDs))

	// Copy min/max values (or leave as nil)
	for i := 0; i < len(idx.columnIDs); i++ {
		if i < len(minValues) && minValues[i] != nil {
			min[i] = minValues[i]
		}
		if i < len(maxValues) && maxValues[i] != nil {
			max[i] = maxValues[i]
		}
	}

	// Result will hold our matching row IDs
	var result []int64

	// For each key, check if it falls within the range for each column
	for keyIndex, key := range idx.valueTree.root.keys {
		inRange := true

		// Check if this key falls within range for each column
		for colIndex := 0; colIndex < len(idx.columnIDs); colIndex++ {
			// Skip checks for columns where neither min nor max is specified
			if (colIndex >= len(min) || min[colIndex] == nil) &&
				(colIndex >= len(max) || max[colIndex] == nil) {
				continue
			}

			// For a key that doesn't have this column or it's NULL, only skip the check
			// if both min and max are set. If one is not set, treat as open range.
			if colIndex >= len(key) || key[colIndex] == nil || key[colIndex].IsNull() {
				// Don't include keys with NULL in columns that have range bounds
				if (colIndex < len(min) && min[colIndex] != nil) ||
					(colIndex < len(max) && max[colIndex] != nil) {
					inRange = false
					break
				}
				continue
			}

			// Check minimum bound if specified
			if colIndex < len(min) && min[colIndex] != nil {
				cmp, err := key[colIndex].Compare(min[colIndex])
				if err != nil || (includeMin && cmp < 0) || (!includeMin && cmp <= 0) {
					inRange = false
					break
				}
			}

			// Check maximum bound if specified
			if colIndex < len(max) && max[colIndex] != nil {
				cmp, err := key[colIndex].Compare(max[colIndex])
				if err != nil || (includeMax && cmp > 0) || (!includeMax && cmp >= 0) {
					inRange = false
					break
				}
			}
		}

		// If the key is in range, add its row IDs to the result
		if inRange {
			rowIDs := idx.valueTree.root.rowIDs[keyIndex]
			result = append(result, rowIDs...)
		}
	}

	return result
}

// FindWithOperator finds all entries that match the given operator and values
func (idx *MultiColumnarIndex) FindWithOperator(op storage.Operator,
	values []storage.ColumnValue) ([]storage.IndexEntry, error) {

	if len(values) == 0 || len(values) > len(idx.columnIDs) {
		return nil, fmt.Errorf("invalid number of values for multi-column index")
	}

	var rowIDs []int64

	switch op {
	case storage.EQ:
		// For equality, all columns must match exactly
		if len(values) != len(idx.columnIDs) {
			// Pad with NULLs if needed
			paddedValues := make([]storage.ColumnValue, len(idx.columnIDs))
			copy(paddedValues, values)
			// Remaining values are nil (NULL)
			rowIDs = idx.GetRowIDsEqual(paddedValues)
		} else {
			rowIDs = idx.GetRowIDsEqual(values)
		}

	case storage.GT, storage.GTE, storage.LT, storage.LTE:
		// For range queries, we need min and max bounds
		var min, max []storage.ColumnValue
		var includeMin, includeMax bool

		if op == storage.GT || op == storage.GTE {
			min = values
			includeMin = op == storage.GTE
		} else {
			max = values
			includeMax = op == storage.LTE
		}

		rowIDs = idx.GetRowIDsInRange(min, max, includeMin, includeMax)

	case storage.ISNULL:
		// Return rows where ANY of the columns is NULL
		idx.mutex.RLock()

		// If we're looking for NULL in a specific column
		if len(values) == 1 && len(idx.columnIDs) > 1 {
			for i, colName := range idx.columnNames {
				if colName == values[0].AsInterface().(string) {
					colID := idx.columnIDs[i]
					if len(idx.nullRowsByColumn[colID]) > 0 {
						rowIDs = make([]int64, len(idx.nullRowsByColumn[colID]))
						copy(rowIDs, idx.nullRowsByColumn[colID])
					}
					break
				}
			}
		} else {
			// Get all rows with NULL in any column, avoiding duplicates
			nullMap := make(map[int64]struct{})
			for _, colID := range idx.columnIDs {
				for _, rowID := range idx.nullRowsByColumn[colID] {
					nullMap[rowID] = struct{}{}
				}
			}

			if len(nullMap) > 0 {
				rowIDs = make([]int64, 0, len(nullMap))
				for rowID := range nullMap {
					rowIDs = append(rowIDs, rowID)
				}
			}
		}
		idx.mutex.RUnlock()

	case storage.ISNOTNULL:
		// Return rows where all columns are non-NULL
		idx.mutex.RLock()
		allRows := idx.valueTree.GetAll()
		idx.mutex.RUnlock()

		if len(allRows) > 0 {
			// Create a map of rows that have NULL in any column
			nullMap := make(map[int64]struct{})
			for _, colID := range idx.columnIDs {
				for _, rowID := range idx.nullRowsByColumn[colID] {
					nullMap[rowID] = struct{}{}
				}
			}

			// Only include rows not in nullMap
			if len(nullMap) == 0 {
				rowIDs = allRows
			} else {
				rowIDs = make([]int64, 0, len(allRows)-len(nullMap))
				for _, rowID := range allRows {
					if _, exists := nullMap[rowID]; !exists {
						rowIDs = append(rowIDs, rowID)
					}
				}
			}
		}
	}

	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// GetFilteredRowIDs returns row IDs that match the given expression
func (idx *MultiColumnarIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	if expr == nil {
		return nil
	}

	// Handle simple expressions
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Find which column this expression is for
		colIndex := -1
		for i, name := range idx.columnNames {
			if simpleExpr.Column == name {
				colIndex = i
				break
			}
		}

		// If we found the column, proceed
		if colIndex >= 0 {
			// Convert the expression value to a column value
			value := storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[colIndex])
			defer storage.PutPooledColumnValue(value)

			// Create a values array with just this value in the correct position
			values := make([]storage.ColumnValue, len(idx.columnIDs))
			values[colIndex] = value

			// Branch based on operator
			switch simpleExpr.Operator {
			case storage.EQ:
				// Acquire a read lock
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()

				// Manual search through all keys
				var result []int64

				// For first column equality, we may be able to do a more focused search
				if colIndex == 0 {
					// Check if we can get the value as a string for easier comparison
					stringValue, isString := simpleExpr.Value.(string)

					// Scan keys to find matches
					for i, key := range idx.valueTree.root.keys {
						if len(key) > 0 && key[0] != nil && !key[0].IsNull() {
							// For string values, direct comparison may be more reliable
							if isString && key[0].Type() == storage.TEXT {
								keyString := key[0].AsInterface().(string)
								if keyString == stringValue {
									result = append(result, idx.valueTree.root.rowIDs[i]...)
								}
							} else {
								// Use column value comparison
								cmp, err := key[0].Compare(value)
								if err == nil && cmp == 0 {
									result = append(result, idx.valueTree.root.rowIDs[i]...)
								}
							}
						}
					}
					return result
				}

				// For other columns, we need to scan all keys and check the specific column
				for i, key := range idx.valueTree.root.keys {
					// Skip keys that don't have the column or where it's NULL
					if colIndex >= len(key) || key[colIndex] == nil || key[colIndex].IsNull() {
						continue
					}

					// Check equality
					cmp, err := key[colIndex].Compare(value)
					if err == nil && cmp == 0 {
						// This key matches, add its row IDs
						result = append(result, idx.valueTree.root.rowIDs[i]...)
					}
				}
				return result

			case storage.GT, storage.GTE, storage.LT, storage.LTE:
				// Set up range query
				var min, max []storage.ColumnValue
				var includeMin, includeMax bool

				if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
					min = values
					includeMin = simpleExpr.Operator == storage.GTE
				} else {
					max = values
					includeMax = simpleExpr.Operator == storage.LTE
				}

				// Acquire a read lock
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()

				if colIndex == 0 {
					// For first column range, we can use optimized range search
					var singleColMin, singleColMax []storage.ColumnValue

					// Only create min/max arrays if the operator requires them
					if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
						singleColMin = []storage.ColumnValue{value}
					} else if simpleExpr.Operator == storage.LT || simpleExpr.Operator == storage.LTE {
						singleColMax = []storage.ColumnValue{value}
					}

					return idx.GetRowIDsInRange(singleColMin, singleColMax, includeMin, includeMax)
				}

				// For other columns, scan all keys
				var result []int64
				for i, key := range idx.valueTree.root.keys {
					// Skip keys that don't have this column or it's NULL
					if colIndex >= len(key) || key[colIndex] == nil || key[colIndex].IsNull() {
						continue
					}

					// Check if this key matches the range condition
					inRange := true

					// Check minimum bound if specified
					if min != nil && min[colIndex] != nil {
						cmp, err := key[colIndex].Compare(min[colIndex])
						if err != nil || (includeMin && cmp < 0) || (!includeMin && cmp <= 0) {
							inRange = false
						}
					}

					// Check maximum bound if specified
					if inRange && max != nil && max[colIndex] != nil {
						cmp, err := key[colIndex].Compare(max[colIndex])
						if err != nil || (includeMax && cmp > 0) || (!includeMax && cmp >= 0) {
							inRange = false
						}
					}

					// This key is in range, add its row IDs
					if inRange {
						result = append(result, idx.valueTree.root.rowIDs[i]...)
					}
				}
				return result

			case storage.ISNULL:
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()

				// Get rows with NULL in specified column
				colID := idx.columnIDs[colIndex]
				if len(idx.nullRowsByColumn[colID]) == 0 {
					return nil
				}
				result := make([]int64, len(idx.nullRowsByColumn[colID]))
				copy(result, idx.nullRowsByColumn[colID])
				return result

			case storage.ISNOTNULL:
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()

				// Get all rows without NULL in specified column
				allRows := idx.valueTree.GetAll()
				colID := idx.columnIDs[colIndex]

				if len(idx.nullRowsByColumn[colID]) == 0 {
					return allRows
				}

				// Create a map of rows with NULL in specified column for quick lookup
				nullMap := make(map[int64]struct{})
				for _, rowID := range idx.nullRowsByColumn[colID] {
					nullMap[rowID] = struct{}{}
				}

				// Only include rows not in nullMap
				result := make([]int64, 0, len(allRows)-len(nullMap))
				for _, rowID := range allRows {
					if _, exists := nullMap[rowID]; !exists {
						result = append(result, rowID)
					}
				}
				return result
			}
		}
	}

	// Handle AND expressions for range queries or multi-column conditions
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		if ok1 && ok2 {
			// Case 1: Range query on same column (e.g., col > 10 AND col < 20)
			if expr1.Column == expr2.Column {
				// Find which column this is in our index
				colIndex := -1
				for i, name := range idx.columnNames {
					if expr1.Column == name {
						colIndex = i
						break
					}
				}

				if colIndex >= 0 {
					// Prepare values arrays
					var min, max []storage.ColumnValue
					var includeMin, includeMax bool
					var hasRange bool

					minValues := make([]storage.ColumnValue, len(idx.columnIDs))
					maxValues := make([]storage.ColumnValue, len(idx.columnIDs))

					// Check if expr1 is a lower bound and expr2 is an upper bound
					if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
						(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {

						minValues[colIndex] = storage.ValueToPooledColumnValue(expr1.Value, idx.dataTypes[colIndex])
						defer storage.PutPooledColumnValue(minValues[colIndex])
						includeMin = expr1.Operator == storage.GTE

						maxValues[colIndex] = storage.ValueToPooledColumnValue(expr2.Value, idx.dataTypes[colIndex])
						defer storage.PutPooledColumnValue(maxValues[colIndex])
						includeMax = expr2.Operator == storage.LTE

						min = minValues
						max = maxValues
						hasRange = true
					}

					// Check if expr2 is a lower bound and expr1 is an upper bound
					if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
						(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {

						minValues[colIndex] = storage.ValueToPooledColumnValue(expr2.Value, idx.dataTypes[colIndex])
						defer storage.PutPooledColumnValue(minValues[colIndex])
						includeMin = expr2.Operator == storage.GTE

						maxValues[colIndex] = storage.ValueToPooledColumnValue(expr1.Value, idx.dataTypes[colIndex])
						defer storage.PutPooledColumnValue(maxValues[colIndex])
						includeMax = expr1.Operator == storage.LTE

						min = minValues
						max = maxValues
						hasRange = true
					}

					if hasRange {
						if colIndex == 0 {
							// For first column, use range query directly
							singleColMin := []storage.ColumnValue{min[colIndex]}
							singleColMax := []storage.ColumnValue{max[colIndex]}
							return idx.GetRowIDsInRange(singleColMin, singleColMax, includeMin, includeMax)
						} else {
							// For other columns, need custom filtering
							idx.mutex.RLock()
							defer idx.mutex.RUnlock()

							var result []int64
							for i, key := range idx.valueTree.root.keys {
								// Skip keys that don't have this column or it's NULL
								if colIndex >= len(key) || key[colIndex] == nil || key[colIndex].IsNull() {
									continue
								}

								// Check minimum bound
								if min[colIndex] != nil {
									cmp, err := key[colIndex].Compare(min[colIndex])
									if err != nil || (includeMin && cmp < 0) || (!includeMin && cmp <= 0) {
										continue
									}
								}

								// Check maximum bound
								if max[colIndex] != nil {
									cmp, err := key[colIndex].Compare(max[colIndex])
									if err != nil || (includeMax && cmp > 0) || (!includeMax && cmp >= 0) {
										continue
									}
								}

								// This key is in range, add its row IDs
								result = append(result, idx.valueTree.root.rowIDs[i]...)
							}
							return result
						}
					}
				}
			}

			// Case 2: Conditions on different columns (e.g., country = 'USA' AND city = 'New York')
			// Find column indices
			col1Index, col2Index := -1, -1
			for i, name := range idx.columnNames {
				if expr1.Column == name {
					col1Index = i
				}
				if expr2.Column == name {
					col2Index = i
				}
			}

			// If both columns are in our index
			if col1Index >= 0 && col2Index >= 0 {
				// Convert expression values to column values
				val1 := storage.ValueToPooledColumnValue(expr1.Value, idx.dataTypes[col1Index])
				defer storage.PutPooledColumnValue(val1)

				val2 := storage.ValueToPooledColumnValue(expr2.Value, idx.dataTypes[col2Index])
				defer storage.PutPooledColumnValue(val2)

				idx.mutex.RLock()
				defer idx.mutex.RUnlock()

				var result []int64

				// Evaluate each expression based on its operator
				evalExpr := func(key []storage.ColumnValue, colIdx int, val storage.ColumnValue, op storage.Operator) (bool, error) {
					// Ensure the key has this column and it's not NULL
					if colIdx >= len(key) || key[colIdx] == nil || key[colIdx].IsNull() {
						return false, nil
					}

					// Compare values
					cmp, err := key[colIdx].Compare(val)
					if err != nil {
						return false, err
					}

					// Evaluate based on operator
					switch op {
					case storage.EQ:
						return cmp == 0, nil
					case storage.GT:
						return cmp > 0, nil
					case storage.GTE:
						return cmp >= 0, nil
					case storage.LT:
						return cmp < 0, nil
					case storage.LTE:
						return cmp <= 0, nil
					default:
						return false, nil
					}
				}

				// For each key in the tree
				for i, key := range idx.valueTree.root.keys {
					// Check first expression
					matches1, err1 := evalExpr(key, col1Index, val1, expr1.Operator)
					if err1 != nil || !matches1 {
						continue
					}

					// Check second expression
					matches2, err2 := evalExpr(key, col2Index, val2, expr2.Operator)
					if err2 != nil || !matches2 {
						continue
					}

					// Both expressions matched, add the row IDs
					result = append(result, idx.valueTree.root.rowIDs[i]...)
				}

				return result
			}
		} else {
			// Try individual expression approach - handle cases where expressions aren't simple expressions
			// Evaluate both sides separately and intersect results
			result1 := idx.GetFilteredRowIDs(andExpr.Expressions[0])
			if len(result1) == 0 {
				return nil // Short-circuit if first expression returns no rows
			}

			result2 := idx.GetFilteredRowIDs(andExpr.Expressions[1])
			if len(result2) == 0 {
				return nil // Short-circuit if second expression returns no rows
			}

			// Find the intersection of the two result sets
			rowMap := make(map[int64]struct{}, len(result1))
			for _, id := range result1 {
				rowMap[id] = struct{}{}
			}

			var result []int64
			for _, id := range result2 {
				if _, exists := rowMap[id]; exists {
					result = append(result, id)
				}
			}

			return result
		}
	}

	// For other expressions, we can't use this index efficiently
	return nil
}

// Build builds the index from existing data
func (idx *MultiColumnarIndex) Build() error {
	// Clear existing data
	idx.mutex.Lock()

	if idx.valueTree != nil {
		idx.valueTree.Clear()
	} else {
		idx.valueTree = newMultiColumnTree(compareMultiColumnValues)
	}

	// Clear nullRowsByColumn
	for colID := range idx.nullRowsByColumn {
		idx.nullRowsByColumn[colID] = make([]int64, 0)
	}
	idx.mutex.Unlock()

	// Get all visible versions from the version store
	if idx.versionStore == nil {
		return nil
	}

	// Use transaction ID 0 to see all committed data
	visibleVersions := idx.versionStore.GetAllVisibleVersions(0)

	// Process each visible row
	visibleVersions.ForEach(func(rowID int64, version *RowVersion) bool {
		if version.IsDeleted {
			return true
		}

		// Extract values for each indexed column
		values := make([]storage.ColumnValue, len(idx.columnIDs))

		for i, colID := range idx.columnIDs {
			if int(colID) < len(version.Data) {
				values[i] = version.Data[colID]
			} else {
				// Column doesn't exist in this row, treat as NULL
				values[i] = nil
			}
		}

		// Add to index
		idx.Add(values, rowID, 0)

		return true
	})

	return nil
}

// Close releases resources held by the index
func (idx *MultiColumnarIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Clear valueTree and nullRowsByColumn to free memory
	if idx.valueTree != nil {
		idx.valueTree.Clear()
		idx.valueTree = nil
	}

	for colID := range idx.nullRowsByColumn {
		idx.nullRowsByColumn[colID] = nil
	}
	idx.nullRowsByColumn = nil

	return nil
}
