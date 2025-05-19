// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"fmt"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/storage"
)

// UniqueConstraintIndex is a lightweight index that ONLY handles
// uniqueness constraints across multiple columns, with no query support
type UniqueConstraintIndex struct {
	name        string
	tableName   string
	columnNames []string
	columnIDs   []int
	dataTypes   []storage.DataType

	// Map for efficient uniqueness checking
	// The key is a binary representation of all column values
	uniqueValues map[string]int64 // Value is the rowID

	mutex sync.RWMutex
}

// NewUniqueConstraintIndex creates a new index for uniqueness constraint enforcement only
func NewUniqueConstraintIndex(
	name string,
	tableName string,
	columnNames []string,
	columnIDs []int,
	dataTypes []storage.DataType) *UniqueConstraintIndex {

	idx := &UniqueConstraintIndex{
		name:         name,
		tableName:    tableName,
		columnNames:  columnNames,
		columnIDs:    columnIDs,
		dataTypes:    dataTypes,
		uniqueValues: make(map[string]int64),
	}

	return idx
}

// Storage.Index interface implementation

// Name returns the index name
func (idx *UniqueConstraintIndex) Name() string {
	return idx.name
}

// TableName returns the table this index belongs to
func (idx *UniqueConstraintIndex) TableName() string {
	return idx.tableName
}

// ColumnNames returns the names of indexed columns
func (idx *UniqueConstraintIndex) ColumnNames() []string {
	return idx.columnNames
}

// ColumnIDs returns the IDs of indexed columns
func (idx *UniqueConstraintIndex) ColumnIDs() []int {
	return idx.columnIDs
}

// DataTypes returns the data types of indexed columns
func (idx *UniqueConstraintIndex) DataTypes() []storage.DataType {
	return idx.dataTypes
}

// IndexType returns the type of the index
func (idx *UniqueConstraintIndex) IndexType() storage.IndexType {
	return "unique" // Using string literal directly as storage.UniqueIndex is not defined
}

// IsUnique returns whether this is a unique index
func (idx *UniqueConstraintIndex) IsUnique() bool {
	return true // Always true for this index type
}

// createKey creates a unique key from column values with minimal allocations
func (idx *UniqueConstraintIndex) createKey(values []storage.ColumnValue) string {
	if values == nil {
		return ""
	}

	// Skip if any value is NULL - NULL values don't violate uniqueness
	for _, val := range values {
		if val == nil || val.IsNull() {
			return ""
		}
	}

	// Get a buffer from the pool
	buf := common.GetBufferPool()
	defer common.PutBufferPool(buf)

	// Simple binary encoding of values
	// Format: <value1-length><value1-bytes><value2-length><value2-bytes>...
	for i, val := range values {
		// Different serialization based on type
		switch idx.dataTypes[i] {
		case storage.INTEGER:
			intVal := val.AsInterface().(int64)
			// Write 8 bytes for int64, big-endian
			buf.B = append(buf.B, byte((intVal >> 56) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 48) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 40) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 32) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 24) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 16) & 0xFF))
			buf.B = append(buf.B, byte((intVal >> 8) & 0xFF))
			buf.B = append(buf.B, byte(intVal & 0xFF))

		case storage.FLOAT:
			// Store string representation of float for consistency
			strVal, _ := val.AsString()
			// Write length prefix and then string bytes
			buf.B = append(buf.B, byte(len(strVal)))
			buf.B = append(buf.B, strVal...)

		case storage.TEXT:
			strVal := val.AsInterface().(string)
			// For longer strings, use 2 bytes for length
			if len(strVal) > 255 {
				buf.B = append(buf.B, 255) // Marker for long string
				buf.B = append(buf.B, byte((len(strVal) >> 8) & 0xFF))
				buf.B = append(buf.B, byte(len(strVal) & 0xFF))
			} else {
				buf.B = append(buf.B, byte(len(strVal)))
			}
			buf.B = append(buf.B, strVal...)

		case storage.BOOLEAN:
			if val.AsInterface().(bool) {
				buf.B = append(buf.B, 1)
			} else {
				buf.B = append(buf.B, 0)
			}

		default:
			// For other types, use string representation
			strVal, _ := val.AsString()
			buf.B = append(buf.B, byte(len(strVal)))
			buf.B = append(buf.B, strVal...)
		}

		// Add a type marker byte between values for safety
		if i < len(values)-1 {
			buf.B = append(buf.B, byte(idx.dataTypes[i]))
		}
	}

	// Convert to string - this is the only unavoidable allocation
	return string(buf.B)
}

// Add adds values to the index, enforcing uniqueness
func (idx *UniqueConstraintIndex) Add(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for unique constraint, got %d",
			len(idx.columnIDs), len(values))
	}

	key := idx.createKey(values)
	if key == "" {
		return nil // Skip NULL values
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Check for uniqueness violation
	if _, exists := idx.uniqueValues[key]; exists {
		return storage.NewUniqueConstraintError(
			idx.name,
			strings.Join(idx.columnNames, ","),
			values[0],
		)
	}

	// Add the key to the index
	idx.uniqueValues[key] = rowID
	return nil
}

// AddBatch adds multiple entries in batch mode
func (idx *UniqueConstraintIndex) AddBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// First check all entries for uniqueness
	keysToAdd := make(map[string]int64)
	for rowID, values := range entries {
		// Validate column count
		if len(values) != len(idx.columnIDs) {
			return fmt.Errorf("expected %d values for unique constraint, got %d",
				len(idx.columnIDs), len(values))
		}

		key := idx.createKey(values)
		if key == "" {
			continue // Skip NULL values
		}

		// Check against existing keys
		if _, exists := idx.uniqueValues[key]; exists {
			return storage.NewUniqueConstraintError(
				idx.name,
				strings.Join(idx.columnNames, ","),
				values[0],
			)
		}

		// Check against other entries in this batch
		if _, exists := keysToAdd[key]; exists {
			return storage.NewUniqueConstraintError(
				idx.name,
				strings.Join(idx.columnNames, ","),
				values[0],
			)
		}

		keysToAdd[key] = rowID
	}

	// If all checks pass, add all entries
	for key, rowID := range keysToAdd {
		idx.uniqueValues[key] = rowID
	}

	return nil
}

// Remove removes values from the index
func (idx *UniqueConstraintIndex) Remove(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for unique constraint, got %d",
			len(idx.columnIDs), len(values))
	}

	key := idx.createKey(values)
	if key == "" {
		return nil // Nothing to do for NULL values
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Only remove if rowID matches (important for concurrent operations)
	if existingRowID, exists := idx.uniqueValues[key]; exists && existingRowID == rowID {
		delete(idx.uniqueValues, key)
	}

	return nil
}

// RemoveBatch removes multiple entries in batch mode
func (idx *UniqueConstraintIndex) RemoveBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	for rowID, values := range entries {
		// Validate column count
		if len(values) != len(idx.columnIDs) {
			return fmt.Errorf("expected %d values for unique constraint, got %d",
				len(idx.columnIDs), len(values))
		}

		key := idx.createKey(values)
		if key == "" {
			continue // Skip NULL values
		}

		// Only remove if rowID matches
		if existingRowID, exists := idx.uniqueValues[key]; exists && existingRowID == rowID {
			delete(idx.uniqueValues, key)
		}
	}

	return nil
}

// These methods implement the required interface but return empty results
// since this index is not meant for querying

// Find returns nil for query operations
func (idx *UniqueConstraintIndex) Find(values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	return nil, nil
}

// FindRange returns nil for query operations
func (idx *UniqueConstraintIndex) FindRange(min, max []storage.ColumnValue,
	minInclusive, maxInclusive bool) ([]storage.IndexEntry, error) {
	return nil, nil
}

// FindWithOperator returns nil for query operations
func (idx *UniqueConstraintIndex) FindWithOperator(op storage.Operator,
	values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	return nil, nil
}

// GetRowIDsEqual returns nil for query operations
func (idx *UniqueConstraintIndex) GetRowIDsEqual(values []storage.ColumnValue) []int64 {
	return nil
}

// GetRowIDsInRange returns nil for query operations
func (idx *UniqueConstraintIndex) GetRowIDsInRange(minValue, maxValue []storage.ColumnValue, 
	includeMin, includeMax bool) []int64 {
	return nil
}

// GetFilteredRowIDs returns nil for query operations
func (idx *UniqueConstraintIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	return nil
}

// Build is a no-op for this simple index
func (idx *UniqueConstraintIndex) Build() error {
	return nil
}

// Close releases any resources
func (idx *UniqueConstraintIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.uniqueValues = nil
	return nil
}