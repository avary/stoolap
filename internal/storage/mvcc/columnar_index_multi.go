// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/btree"
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// MultiColumnarIndex represents a high-performance columnar index
type MultiColumnarIndex struct {
	// name is the index name
	name string

	// columnNames are the names of the columns this index is for
	columnNames []string

	// columnIDs are the positions of the columns in the schema
	columnIDs []int

	// dataTypes are the types of the columns
	dataTypes []storage.DataType

	// valueTree stores a mapping from column values to row IDs
	// The key is a multi-column value wrapper, and the value is a slice of row IDs
	valueTree *btree.BTree[*MultiColumnKey, []int64]

	// nullRowsByColumn tracks rows with NULL in each column
	// The keys are column IDs, and values are slices of row IDs with NULL in that column
	nullRowsByColumn map[int][]int64

	// Create a mutex for thread-safety
	mutex sync.RWMutex

	// tableName is the name of the table this index belongs to
	tableName string

	// versionStore is a reference to the MVCC version store
	versionStore *VersionStore

	// isUnique indicates if this is a unique index
	isUnique bool

	// isPrimaryKey indicates if this is the primary key column
	isPrimaryKey bool

	// timeBucketGranularity specifies the granularity for time bucketing
	// This is only used for timestamp columns
	timeBucketGranularity TimeBucketGranularity

	// enableTimeBucketing indicates whether time bucketing is enabled
	enableTimeBucketing bool
}

// MultiColumnKey is a wrapper for multiple column values that can be used as a key in a B-tree
type MultiColumnKey struct {
	// Values are the column values for this key
	Values []storage.ColumnValue

	// DataTypes are the data types of the column values
	DataTypes []storage.DataType
}

// Compare implements btree.Comparer interface for B-tree operations
func (k *MultiColumnKey) Compare(other *MultiColumnKey) int {
	// Compare each column value in order
	minLen := len(k.Values)
	if len(other.Values) < minLen {
		minLen = len(other.Values)
	}

	for i := 0; i < minLen; i++ {
		// Handle NULL values
		aIsNull := k.Values[i] == nil || k.Values[i].IsNull()
		bIsNull := other.Values[i] == nil || other.Values[i].IsNull()

		// NULL values are considered less than non-NULL values
		if aIsNull && !bIsNull {
			return -1
		}
		if !aIsNull && bIsNull {
			return 1
		}
		if aIsNull && bIsNull {
			continue // Both NULL, move to next column
		}

		// Compare non-NULL values using ColumnValue.Compare
		cmp, err := k.Values[i].Compare(other.Values[i])
		if err != nil {
			// Handle error gracefully - use string comparison as fallback
			aStr, _ := k.Values[i].AsString()
			bStr, _ := other.Values[i].AsString()
			if aStr < bStr {
				return -1
			}
			if aStr > bStr {
				return 1
			}
		} else if cmp != 0 {
			return cmp
		}
		// If equal, continue to next column
	}

	// If we get here, all common columns are equal
	// The shorter key is considered less than the longer key
	if len(k.Values) < len(other.Values) {
		return -1
	}
	if len(k.Values) > len(other.Values) {
		return 1
	}

	// Keys are completely equal
	return 0
}

// StartsWithPrefix checks if this key starts with the given prefix values
func (k *MultiColumnKey) StartsWithPrefix(prefix *MultiColumnKey) bool {
	// Short circuit if prefix is longer than key
	if len(prefix.Values) > len(k.Values) {
		return false
	}

	// Check each column in the prefix
	for i := 0; i < len(prefix.Values); i++ {
		// Handle NULL values
		aIsNull := k.Values[i] == nil || k.Values[i].IsNull()
		bIsNull := prefix.Values[i] == nil || prefix.Values[i].IsNull()

		// If both NULL, continue
		if aIsNull && bIsNull {
			continue
		}

		// If one is NULL and the other isn't, they don't match
		if aIsNull != bIsNull {
			return false
		}

		// Compare non-NULL values
		cmp, err := k.Values[i].Compare(prefix.Values[i])
		if err != nil {
			// Handle error gracefully - use string comparison as fallback
			aStr, _ := k.Values[i].AsString()
			bStr, _ := prefix.Values[i].AsString()
			if aStr != bStr {
				return false
			}
		} else if cmp != 0 {
			return false
		}
	}

	// If we get here, all prefix columns match
	return true
}

// NewMultiColumnarIndex creates a new multi-column index with B-tree storage
func NewMultiColumnarIndex(name, tableName string,
	columnNames []string, columnIDs []int, dataTypes []storage.DataType,
	versionStore *VersionStore, isUnique bool) *MultiColumnarIndex {

	if len(columnNames) == 0 || len(columnIDs) == 0 || len(dataTypes) == 0 {
		panic("columnar index requires at least one column")
	}

	if len(columnNames) != len(columnIDs) || len(columnNames) != len(dataTypes) {
		panic("column names, IDs, and types must have the same length")
	}

	// Initialize null tracking for each column
	nullRowsByColumn := make(map[int][]int64)
	for _, colID := range columnIDs {
		nullRowsByColumn[colID] = make([]int64, 0, 16)
	}

	// Create a custom B-tree with our MultiColumnKey
	valueTree := btree.NewBTree[*MultiColumnKey, []int64]()

	idx := &MultiColumnarIndex{
		name:                  name,
		columnNames:           columnNames,
		columnIDs:             columnIDs,
		dataTypes:             dataTypes,
		valueTree:             valueTree,
		nullRowsByColumn:      nullRowsByColumn,
		mutex:                 sync.RWMutex{},
		tableName:             tableName,
		versionStore:          versionStore,
		isUnique:              isUnique,
		isPrimaryKey:          false,
		timeBucketGranularity: DayBucket,
		enableTimeBucketing:   false,
	}

	return idx
}

// EnableTimeBucketing enables time bucketing for timestamp columns
func (idx *MultiColumnarIndex) EnableTimeBucketing(granularity TimeBucketGranularity) {
	// Only enable for single-column timestamp indexes or when first column is timestamp
	if len(idx.dataTypes) == 0 || idx.dataTypes[0] != storage.TIMESTAMP {
		return
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.timeBucketGranularity = granularity
	idx.enableTimeBucketing = true
}

// DisableTimeBucketing disables time bucketing
func (idx *MultiColumnarIndex) DisableTimeBucketing() {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.enableTimeBucketing = false
}

// Name returns the index name - implements IndexInterface
func (idx *MultiColumnarIndex) Name() string {
	return idx.name
}

// IndexType returns the type of the index - implements IndexInterface
func (idx *MultiColumnarIndex) IndexType() storage.IndexType {
	return storage.ColumnarIndex
}

// ColumnNames returns the names of the columns this index is for - implements IndexInterface
func (idx *MultiColumnarIndex) ColumnNames() []string {
	return idx.columnNames
}

// ColumnID returns the ID of the first column in this index
func (idx *MultiColumnarIndex) ColumnID() int {
	if len(idx.columnIDs) > 0 {
		return idx.columnIDs[0]
	}
	return -1
}

// ColumnIDs returns all column IDs for this index
func (idx *MultiColumnarIndex) ColumnIDs() []int {
	return idx.columnIDs
}

// Add adds a value to the index with the given row ID - implements IndexInterface
func (idx *MultiColumnarIndex) Add(value storage.ColumnValue, rowID int64, refID int64) error {
	// Add to multi-column index as a single-column value
	return idx.AddMulti([]storage.ColumnValue{value}, rowID, refID)
}

// AddMulti adds multiple values to the index for multi-column indexes - implements IndexInterface
func (idx *MultiColumnarIndex) AddMulti(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate we have the right number of values
	if len(values) != len(idx.columnNames) {
		return fmt.Errorf("expected %d values for columns, got %d values",
			len(idx.columnNames), len(values))
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Check for NULL values in any column
	hasNull := false
	for i, value := range values {
		if value == nil || value.IsNull() {
			hasNull = true
			// Store the rowID in the null map for this column
			colID := idx.columnIDs[i]
			idx.nullRowsByColumn[colID] = append(idx.nullRowsByColumn[colID], rowID)
		}
	}

	// Create a MultiColumnKey for these values
	key := &MultiColumnKey{
		Values:    values,
		DataTypes: idx.dataTypes,
	}

	// For unique constraints, check if these values already exist
	if idx.isUnique && !hasNull {
		existingRows, found := idx.valueTree.Search(key)
		if found && len(existingRows) > 0 {
			// Unique constraint violation
			colNames := strings.Join(idx.columnNames, ",")
			return storage.NewUniqueConstraintError(idx.name, colNames,
				storage.NewStringValue(fmt.Sprintf("%v", values)))
		}
	}

	// Add to the index
	// First, check if we already have entries for this key
	existingRows, found := idx.valueTree.Search(key)

	var newRows []int64
	if found {
		newRows = existingRows
		newRows = append(newRows, rowID)
	} else {
		// Create a new slice with just this row ID
		newRows = []int64{rowID}
	}

	// Update the tree with the new or modified row ID list
	idx.valueTree.Insert(key, newRows)

	return nil
}

// Find finds all pairs where the column equals the given value - implements IndexInterface
func (idx *MultiColumnarIndex) Find(value storage.ColumnValue) ([]storage.IndexEntry, error) {
	// Get matching row IDs
	rowIDs := idx.GetRowIDsEqual(value)
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

// FindMulti finds all pairs where multiple columns match given values - implements IndexInterface
func (idx *MultiColumnarIndex) FindMulti(values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	// Get matching row IDs
	rowIDs := idx.GetRowIDsEqualMulti(values)
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

// ForEachRowIDEqual applies a callback function to each row ID with the given value for the first column
func (idx *MultiColumnarIndex) ForEachRowIDEqual(value storage.ColumnValue, callback func(int64) bool) {
	// Use the first column value as a prefix search
	idx.ForEachRowIDPrefixMatch([]storage.ColumnValue{value}, callback)
}

// GetRowIDsEqual returns row IDs with the given value for the first column
func (idx *MultiColumnarIndex) GetRowIDsEqual(value storage.ColumnValue) []int64 {
	var result []int64
	idx.ForEachRowIDEqual(value, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// ForEachRowIDEqualMulti applies a callback function to each row ID matching multiple column values exactly
func (idx *MultiColumnarIndex) ForEachRowIDEqualMulti(values []storage.ColumnValue, callback func(int64) bool) {
	// Validate value count
	if len(values) != len(idx.columnNames) {
		return
	}

	// Check if any values are NULL
	for i, value := range values {
		if value == nil || value.IsNull() {
			// For NULL in any column, return rows that have NULL in that position
			idx.mutex.RLock()
			colID := idx.columnIDs[i]
			nullRows := idx.nullRowsByColumn[colID]

			// Process null rows
			if len(nullRows) > 0 {
				for _, rowID := range nullRows {
					if !callback(rowID) {
						break // Stop iteration if callback returns false
					}
				}
			}
			idx.mutex.RUnlock()
			return
		}
	}

	// Create a full key to search for
	key := &MultiColumnKey{
		Values:    values,
		DataTypes: idx.dataTypes,
	}

	// Lock for reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Look up rows with this exact key
	rowIDs, found := idx.valueTree.Search(key)
	if found && len(rowIDs) > 0 {
		for _, rowID := range rowIDs {
			if !callback(rowID) {
				break // Stop iteration if callback returns false
			}
		}
	}
}

// GetRowIDsEqualMulti returns row IDs matching multiple column values exactly
func (idx *MultiColumnarIndex) GetRowIDsEqualMulti(values []storage.ColumnValue) []int64 {
	var result []int64
	idx.ForEachRowIDEqualMulti(values, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// ForEachRowIDPrefixMatch applies a callback function to each row ID that matches a prefix of columns
func (idx *MultiColumnarIndex) ForEachRowIDPrefixMatch(prefixValues []storage.ColumnValue, callback func(int64) bool) {
	// Validate values count is <= columns count
	if len(prefixValues) > len(idx.columnNames) {
		return
	}

	// Check if any values are NULL
	for i, value := range prefixValues {
		if value == nil || value.IsNull() {
			// For NULL in any column, return rows that have NULL in that position
			idx.mutex.RLock()
			colID := idx.columnIDs[i]
			nullRows := idx.nullRowsByColumn[colID]

			// Process null rows
			if len(nullRows) > 0 {
				for _, rowID := range nullRows {
					if !callback(rowID) {
						break // Stop iteration if callback returns false
					}
				}
			}
			idx.mutex.RUnlock()
			return
		}
	}

	// If it's an exact match (all columns provided), use the exact match function
	if len(prefixValues) == len(idx.columnNames) {
		idx.ForEachRowIDEqualMulti(prefixValues, callback)
		return
	}

	// Create a prefix key to search for
	prefixKey := GetMultiColumnKey(len(prefixValues))
	prefixKey.Values = prefixKey.Values[:len(prefixValues)]
	prefixKey.DataTypes = prefixKey.DataTypes[:len(prefixValues)]
	defer PutMultiColumnKey(prefixKey)

	for i, val := range prefixValues {
		prefixKey.Values[i] = val
		prefixKey.DataTypes[i] = idx.dataTypes[i]
	}

	// Lock for reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Track processed rows to avoid duplicates
	seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])
	defer func() {
		seen.Clear()
		int64StructMapPool.Put(seen)
	}()

	// For each entry in the tree
	idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
		// Check if this key starts with our prefix
		if key.StartsWithPrefix(prefixKey) {
			// Process each row ID for this key
			for _, rowID := range rowIDs {
				// Skip if already processed (deduplication)
				if alreadySeen := seen.Has(rowID); alreadySeen {
					continue
				}
				seen.Put(rowID, struct{}{})

				// Apply callback
				if !callback(rowID) {
					return false // Stop iteration if callback returns false
				}
			}
		}
		return true // Continue iterating
	})
}

// GetRowIDsPrefixMatch returns row IDs that match a prefix of columns
func (idx *MultiColumnarIndex) GetRowIDsPrefixMatch(prefixValues []storage.ColumnValue) []int64 {
	var result []int64
	idx.ForEachRowIDPrefixMatch(prefixValues, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// FindRange finds all row IDs where the column is in the given range - implements IndexInterface
func (idx *MultiColumnarIndex) FindRange(minValue, maxValue storage.ColumnValue,
	includeMin, includeMax bool) ([]storage.IndexEntry, error) {

	// Get row IDs in the range
	rowIDs := idx.GetRowIDsInRange(minValue, maxValue, includeMin, includeMax)
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

// FindRangeMulti finds rows where multiple columns match given range conditions - implements IndexInterface
func (idx *MultiColumnarIndex) FindRangeMulti(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool) ([]storage.IndexEntry, error) {

	// Get row IDs in the range
	rowIDs := idx.GetRowIDsInRangeMulti(minValues, maxValues, includeMin, includeMax)
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

// ForEachRowIDInRange applies a callback to row IDs with values in the given range for the first column
func (idx *MultiColumnarIndex) ForEachRowIDInRange(minValue, maxValue storage.ColumnValue,
	includeMin, includeMax bool, callback func(int64) bool) {

	// For multi-column indexes, delegate to the multi-column range search
	idx.ForEachRowIDInRangeMulti(
		[]storage.ColumnValue{minValue},
		[]storage.ColumnValue{maxValue},
		includeMin, includeMax, callback)
}

// GetRowIDsInRange returns row IDs with values in the given range for the first column
func (idx *MultiColumnarIndex) GetRowIDsInRange(minValue, maxValue storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	var result []int64
	idx.ForEachRowIDInRange(minValue, maxValue, includeMin, includeMax, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// ForEachRowIDInRangeMulti applies a callback to row IDs with values in the given multi-column range
func (idx *MultiColumnarIndex) ForEachRowIDInRangeMulti(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool, callback func(int64) bool) {

	// Validate column counts
	if len(minValues) > len(idx.columnNames) || len(maxValues) > len(idx.columnNames) {
		return
	}

	// Get the number of columns to use (minimum of provided min/max values)
	colCount := len(minValues)
	if len(maxValues) < colCount {
		colCount = len(maxValues)
	}

	// If we have no columns to compare, return
	if colCount == 0 {
		return
	}

	// Create min and max keys to define our range
	minKey := &MultiColumnKey{
		Values:    make([]storage.ColumnValue, colCount),
		DataTypes: make([]storage.DataType, colCount),
	}
	maxKey := &MultiColumnKey{
		Values:    make([]storage.ColumnValue, colCount),
		DataTypes: make([]storage.DataType, colCount),
	}

	// Fill in the keys
	for i := 0; i < colCount; i++ {
		minKey.Values[i] = minValues[i]
		minKey.DataTypes[i] = idx.dataTypes[i]
		maxKey.Values[i] = maxValues[i]
		maxKey.DataTypes[i] = idx.dataTypes[i]
	}

	// Lock for reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Track processed rows to avoid duplicates
	seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])
	defer func() {
		seen.Clear()
		int64StructMapPool.Put(seen)
	}()

	// For each entry in the tree
	idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
		// Check each value to see if it's in range
		inRange := true

		// Ensure the key has enough columns
		if len(key.Values) < colCount {
			inRange = false
		} else {
			// Check each column to see if it's in range
			for i := 0; i < colCount; i++ {
				// Skip NULL values
				if key.Values[i] == nil || key.Values[i].IsNull() {
					continue
				}

				// Check min bound if specified
				if minValues[i] != nil && !minValues[i].IsNull() {
					cmp, err := key.Values[i].Compare(minValues[i])
					if err != nil || cmp < 0 || (cmp == 0 && !includeMin) {
						inRange = false
						break
					}
				}

				// Check max bound if specified
				if maxValues[i] != nil && !maxValues[i].IsNull() {
					cmp, err := key.Values[i].Compare(maxValues[i])
					if err != nil || cmp > 0 || (cmp == 0 && !includeMax) {
						inRange = false
						break
					}
				}
			}
		}

		// If in range, process all rows
		if inRange {
			for _, rowID := range rowIDs {
				// Skip if already processed (deduplication)
				if alreadySeen := seen.Has(rowID); alreadySeen {
					continue
				}
				seen.Put(rowID, struct{}{})

				// Apply callback
				if !callback(rowID) {
					return false // Stop iteration if callback returns false
				}
			}
		}
		return true // Continue iterating
	})
}

// GetRowIDsInRangeMulti returns row IDs with values in the given multi-column range
func (idx *MultiColumnarIndex) GetRowIDsInRangeMulti(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	var result []int64
	idx.ForEachRowIDInRangeMulti(minValues, maxValues, includeMin, includeMax, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// ForEachLatestBefore applies a callback to the most recent row IDs before a given timestamp
func (idx *MultiColumnarIndex) ForEachLatestBefore(timestamp time.Time, callback func(int64) bool) {
	// Only works for single-column timestamp indexes or multi-column indexes with timestamp as first column
	if len(idx.dataTypes) == 0 || idx.dataTypes[0] != storage.TIMESTAMP {
		return
	}

	// Lock for reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Track processed rows to avoid duplicates
	seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])
	defer func() {
		seen.Clear()
		int64StructMapPool.Put(seen)
	}()

	// For each entry in the tree
	idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
		// Check if the key's timestamp is <= the target timestamp
		if len(key.Values) > 0 && key.Values[0] != nil && !key.Values[0].IsNull() {
			if keyTime, ok := key.Values[0].AsTimestamp(); ok {
				// Process rows with timestamp <= the target
				if !keyTime.After(timestamp) {
					for _, rowID := range rowIDs {
						// Skip if already processed (deduplication)
						if alreadySeen := seen.Has(rowID); alreadySeen {
							continue
						}
						seen.Put(rowID, struct{}{})

						// Apply callback
						if !callback(rowID) {
							return false // Stop iteration if callback returns false
						}
					}
				}
			}
		}
		return true // Continue iterating
	})
}

// GetLatestBefore finds the most recent row IDs before a given timestamp
func (idx *MultiColumnarIndex) GetLatestBefore(timestamp time.Time) []int64 {
	var result []int64
	idx.ForEachLatestBefore(timestamp, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// ForEachRecentTimeRange applies a callback to row IDs within a recent time window
func (idx *MultiColumnarIndex) ForEachRecentTimeRange(duration time.Duration, callback func(int64) bool) {
	// Only works for single-column timestamp indexes or multi-column indexes with timestamp as first column
	if len(idx.dataTypes) == 0 || idx.dataTypes[0] != storage.TIMESTAMP {
		return
	}

	now := time.Now()
	startTime := now.Add(-duration)

	// Convert to timestamp values
	startValue := storage.NewTimestampValue(startTime)
	endValue := storage.NewTimestampValue(now)

	// Use range query to find rows in the time range
	idx.ForEachRowIDInRange(startValue, endValue, true, true, callback)
}

// GetRecentTimeRange finds row IDs within a recent time window (e.g., last hour, day)
func (idx *MultiColumnarIndex) GetRecentTimeRange(duration time.Duration) []int64 {
	var result []int64
	idx.ForEachRecentTimeRange(duration, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// Remove removes a value from the index - implements Index interface
func (idx *MultiColumnarIndex) Remove(value storage.ColumnValue, rowID int64, refID int64) error {
	// For single column, delegate to multi-column remove
	return idx.RemoveMulti([]storage.ColumnValue{value}, rowID, refID)
}

// RemoveMulti removes multiple values from the index - implements IndexInterface
func (idx *MultiColumnarIndex) RemoveMulti(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate values count
	if len(values) != len(idx.columnNames) {
		return fmt.Errorf("expected %d values for columns, got %d values",
			len(idx.columnNames), len(values))
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// First, check for NULL values
	for i, value := range values {
		if value == nil || value.IsNull() {
			// Remove from the null rows for this column
			colID := idx.columnIDs[i]
			nullRows := idx.nullRowsByColumn[colID]

			for j, id := range nullRows {
				if id == rowID {
					// Remove by swapping with the last element and truncating
					nullRows[j] = nullRows[len(nullRows)-1]
					idx.nullRowsByColumn[colID] = nullRows[:len(nullRows)-1]
					break
				}
			}
		}
	}

	// Create a key to search for
	key := &MultiColumnKey{
		Values:    values,
		DataTypes: idx.dataTypes,
	}

	// Look up rows with this key
	existingRows, found := idx.valueTree.Search(key)
	if !found || len(existingRows) == 0 {
		// No entry for this key, nothing to remove
		return nil
	}

	// Create a new slice without this rowID
	newRows := make([]int64, 0, len(existingRows)-1)
	for _, id := range existingRows {
		if id != rowID {
			newRows = append(newRows, id)
		}
	}

	// If we have no rows left, remove the key entry completely
	if len(newRows) == 0 {
		idx.valueTree.Delete(key)
	} else {
		// Otherwise, update with the new rows
		idx.valueTree.Insert(key, newRows)
	}

	return nil
}

// ForEachFilteredRowID applies a callback to row IDs that match the given expression
func (idx *MultiColumnarIndex) ForEachFilteredRowID(expr storage.Expression, callback func(int64) bool) {
	if expr == nil {
		return
	}

	// Fast path for wrapped expressions
	if schemaExpr, ok := expr.(*expression.SchemaAwareExpression); ok && schemaExpr.Expr != nil {
		// Directly process the inner expression
		idx.ForEachFilteredRowID(schemaExpr.Expr, callback)
		return
	}

	// Fast path for simple expressions on columns
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Check if this is for one of our columns
		for i, colName := range idx.columnNames {
			if simpleExpr.Column == colName {
				// Found our column - handle based on position
				if i == 0 {
					// First column - use optimized implementation
					idx.forEachRowIDFirstColumnPredicate(simpleExpr, callback)
					return
				} else {
					// Non-first column - use filtering
					idx.forEachRowIDByColumnPredicate(i, simpleExpr, callback)
					return
				}
			}
		}

		// Column not in our index
		return
	}

	// Handle AND expressions (WHERE col1 = X AND col2 = Y)
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		idx.forEachRowIDAndExpression(andExpr, callback)
		return
	}

	// For other expressions, return (not supported by this index)
}

// GetFilteredRowIDs returns row IDs that match the given expression
func (idx *MultiColumnarIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	// Get a pre-allocated slice from the pool
	result := GetCandidateRows()

	// Define a callback that uses our pooled result slice
	// Avoid capturing additional variables in the closure
	callback := func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	}

	// Apply the filter using our callback
	idx.ForEachFilteredRowID(expr, callback)

	// Create a new slice to return to the caller
	// This allows us to return the pooled slice back to the pool
	resultCopy := make([]int64, len(result))
	copy(resultCopy, result)

	// Return the pooled slice
	PutCandidateRows(result)

	return resultCopy
}

// forEachRowIDFirstColumnPredicate applies a callback to row IDs that match a predicate on the first column
func (idx *MultiColumnarIndex) forEachRowIDFirstColumnPredicate(expr *expression.SimpleExpression, callback func(int64) bool) {
	// Handle each operator efficiently
	switch expr.Operator {
	case storage.EQ:
		// Direct equality match on first column
		valueCol := storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[0])
		defer storage.PutPooledColumnValue(valueCol)

		idx.ForEachRowIDEqual(valueCol, callback)

	case storage.GT, storage.GTE, storage.LT, storage.LTE:
		// Range query optimization
		var minValue, maxValue storage.ColumnValue
		var includeMin, includeMax bool

		// Set range bounds based on operator
		if expr.Operator == storage.GT || expr.Operator == storage.GTE {
			minValue = storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[0])
			defer storage.PutPooledColumnValue(minValue)
			includeMin = expr.Operator == storage.GTE
			idx.ForEachRowIDInRange(minValue, nil, includeMin, false, callback)
		} else {
			maxValue = storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[0])
			defer storage.PutPooledColumnValue(maxValue)
			includeMax = expr.Operator == storage.LTE
			idx.ForEachRowIDInRange(nil, maxValue, false, includeMax, callback)
		}

	case storage.ISNULL:
		// NULL check - fast path with precomputed nullRows
		idx.mutex.RLock()
		colID := idx.columnIDs[0]
		nullRows := idx.nullRowsByColumn[colID]

		// Process null rows
		if len(nullRows) > 0 {
			for _, rowID := range nullRows {
				if !callback(rowID) {
					break // Stop iteration if callback returns false
				}
			}
		}
		idx.mutex.RUnlock()

	case storage.ISNOTNULL:
		// Lock for reading
		idx.mutex.RLock()
		defer idx.mutex.RUnlock()

		// Get NULL rows for first column
		colID := idx.columnIDs[0]
		nullRowsSet := make(map[int64]struct{})
		for _, id := range idx.nullRowsByColumn[colID] {
			nullRowsSet[id] = struct{}{}
		}

		// Track processed rows to avoid duplicates
		seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])
		defer func() {
			seen.Clear()
			int64StructMapPool.Put(seen)
		}()

		// For each entry in the tree
		idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
			// If the first column is not NULL, process all row IDs
			if !(key.Values[0] == nil || key.Values[0].IsNull()) {
				for _, rowID := range rowIDs {
					// Skip if it's in the NULL set or already processed
					if _, isNull := nullRowsSet[rowID]; isNull {
						continue
					}
					if alreadySeen := seen.Has(rowID); alreadySeen {
						continue
					}
					seen.Put(rowID, struct{}{})

					// Apply callback
					if !callback(rowID) {
						return false // Stop iteration if callback returns false
					}
				}
			}
			return true // Continue iterating
		})
	}
}

// handleFirstColumnPredicate handles predicates on the first column and returns the result as a slice
func (idx *MultiColumnarIndex) handleFirstColumnPredicate(expr *expression.SimpleExpression) []int64 {
	var result []int64
	idx.forEachRowIDFirstColumnPredicate(expr, func(rowID int64) bool {
		result = append(result, rowID)
		return true // Continue iteration
	})
	return result
}

// forEachRowIDByColumnPredicate applies a callback to row IDs that match a predicate for a non-first column
func (idx *MultiColumnarIndex) forEachRowIDByColumnPredicate(colIdx int, expr *expression.SimpleExpression, callback func(int64) bool) {
	// Acquire read lock
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Fast path: Early exit for NULL checks which can use pre-computed nullRows
	if expr.Operator == storage.ISNULL {
		colID := idx.columnIDs[colIdx]
		nullRows := idx.nullRowsByColumn[colID]

		// Process null rows
		for _, rowID := range nullRows {
			if !callback(rowID) {
				return // Stop if callback returns false
			}
		}
		return
	}

	// Create predicate function for the column
	predicate := createPredicateForColumn(expr, idx.dataTypes[colIdx])

	// Track processed rows to avoid duplicates
	seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])
	defer func() {
		seen.Clear()
		int64StructMapPool.Put(seen)
	}()

	// For each entry in the tree
	idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
		// Skip if the key doesn't have enough columns
		if colIdx >= len(key.Values) {
			return true
		}

		// Apply predicate to the column value
		if predicate(key.Values[colIdx]) {
			// Process each row ID for this key
			for _, rowID := range rowIDs {
				// Skip if already processed (deduplication)
				if alreadySeen := seen.Has(rowID); alreadySeen {
					continue
				}
				seen.Put(rowID, struct{}{})

				// Apply callback
				if !callback(rowID) {
					return false // Stop iteration if callback returns false
				}
			}
		}
		return true // Continue iterating
	})
}

// forEachRowIDAndExpression applies a callback to all row IDs matching an AND expression
func (idx *MultiColumnarIndex) forEachRowIDAndExpression(andExpr *expression.AndExpression, callback func(int64) bool) {
	// Analyze the expressions to find column conditions
	columnConditions := make(map[string]*expression.SimpleExpression, len(idx.columnNames))

	// Collect simple expressions by column name
	for _, subExpr := range andExpr.Expressions {
		// Handle wrapped expressions
		if schemaExpr, ok := subExpr.(*expression.SchemaAwareExpression); ok && schemaExpr.Expr != nil {
			subExpr = schemaExpr.Expr
		}

		// Process simple expressions
		if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
			// Check if this is one of our indexed columns
			for _, colName := range idx.columnNames {
				if simpleExpr.Column == colName {
					// For equality on first column, always prefer it
					if colName == idx.columnNames[0] && simpleExpr.Operator == storage.EQ {
						columnConditions[colName] = simpleExpr
					} else if _, found := columnConditions[colName]; !found {
						// Otherwise, use the first condition found for each column
						columnConditions[colName] = simpleExpr
					}
				}
			}
		}
	}

	// If we didn't find any matching columns, return
	if len(columnConditions) == 0 {
		return
	}

	// Fast path: Equality on leading columns
	// This is the most common case and we can optimize it heavily
	if firstColExpr, hasFirst := columnConditions[idx.columnNames[0]]; hasFirst && firstColExpr.Operator == storage.EQ {
		// Pre-allocate capacity for prefix values to avoid reallocation
		prefixValues := make([]storage.ColumnValue, 0, len(idx.columnNames))

		// Try to get as many consecutive equal columns as possible
		for i, colName := range idx.columnNames {
			expr, found := columnConditions[colName]
			if !found || expr.Operator != storage.EQ {
				break
			}

			// Add the equality value
			value := storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[i])
			defer storage.PutPooledColumnValue(value)
			prefixValues = append(prefixValues, value)
		}

		// If we have values, use prefix match
		if len(prefixValues) > 0 {
			// If we have no more conditions or exact match on all columns, directly use prefix match
			if len(prefixValues) == len(columnConditions) || len(prefixValues) == len(idx.columnNames) {
				idx.ForEachRowIDPrefixMatch(prefixValues, callback)
				return
			}

			// Otherwise, perform a single pass and filter on the in-memory set
			idx.mutex.RLock()

			// Get pooled objects
			rowToKey := int64KeyMapPool.Get().(*fastmap.Int64Map[*MultiColumnKey])
			seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])

			// Get a pre-allocated slice from the pool for candidate rows
			candidateRows := GetCandidateRows()

			// Create a prefix key to search for
			prefixKey := GetMultiColumnKey(len(prefixValues))
			prefixKey.Values = prefixKey.Values[:len(prefixValues)]
			prefixKey.DataTypes = prefixKey.DataTypes[:len(prefixValues)]
			defer PutMultiColumnKey(prefixKey)

			for i, val := range prefixValues {
				prefixKey.Values[i] = val
				prefixKey.DataTypes[i] = idx.dataTypes[i]
			}

			// Find all rows matching the prefix in a single traversal
			// Use a fixed closure to avoid recreating the function for each ForEach call
			idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
				if key.StartsWithPrefix(prefixKey) {
					// Reuse key reference for all rows to avoid storing multiple copies
					k := key // Local reference to avoid multiple map lookups

					// Pre-check capacity and grow candidateRows if necessary
					remainingCap := cap(candidateRows) - len(candidateRows)
					if remainingCap < len(rowIDs) {
						// Grow by doubling or by the number of items needed, whichever is larger
						newCap := cap(candidateRows) * 2
						if newCap < len(candidateRows)+len(rowIDs) {
							newCap = len(candidateRows) + len(rowIDs)
						}
						newSlice := make([]int64, len(candidateRows), newCap)
						copy(newSlice, candidateRows)
						candidateRows = newSlice
					}

					// Process all rowIDs in this key
					for _, rowID := range rowIDs {
						if alreadySeen := seen.Has(rowID); !alreadySeen {
							seen.Put(rowID, struct{}{})
							candidateRows = append(candidateRows, rowID)
							rowToKey.Put(rowID, k)
						}
					}
				}
				return true
			})

			idx.mutex.RUnlock()

			// Process the candidates with cleanup to ensure resources are released
			matchFound := false
			for _, rowID := range candidateRows {
				if idx.matchesRemainingConditions(rowID, columnConditions, prefixValues, rowToKey) {
					matchFound = callback(rowID)
					if !matchFound {
						break // Stop if callback returns false
					}
				}
			}

			// Cleanup pooled resources
			seen.Clear()
			int64StructMapPool.Put(seen)
			rowToKey.Clear()
			int64KeyMapPool.Put(rowToKey)
			PutCandidateRows(candidateRows)

			return
		}
	}

	// Handle range conditions on first column
	if firstColExpr, hasFirst := columnConditions[idx.columnNames[0]]; hasFirst {
		if firstColExpr.Operator == storage.GT || firstColExpr.Operator == storage.GTE ||
			firstColExpr.Operator == storage.LT || firstColExpr.Operator == storage.LTE {

			// Find a complementary range condition
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool
			var hasRange bool

			// Set initial range based on the first expression
			if firstColExpr.Operator == storage.GT || firstColExpr.Operator == storage.GTE {
				minValue = storage.ValueToPooledColumnValue(firstColExpr.Value, idx.dataTypes[0])
				defer storage.PutPooledColumnValue(minValue)
				includeMin = firstColExpr.Operator == storage.GTE
				hasRange = true
			} else {
				maxValue = storage.ValueToPooledColumnValue(firstColExpr.Value, idx.dataTypes[0])
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = firstColExpr.Operator == storage.LTE
				hasRange = true
			}

			// Look for complementary range condition
			for _, subExpr := range andExpr.Expressions {
				// Handle wrapped expressions
				if schemaExpr, ok := subExpr.(*expression.SchemaAwareExpression); ok && schemaExpr.Expr != nil {
					subExpr = schemaExpr.Expr
				}

				if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok &&
					simpleExpr != firstColExpr && simpleExpr.Column == idx.columnNames[0] {

					// Complete the range with complementary condition
					if minValue != nil && (simpleExpr.Operator == storage.LT || simpleExpr.Operator == storage.LTE) {
						maxValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[0])
						defer storage.PutPooledColumnValue(maxValue)
						includeMax = simpleExpr.Operator == storage.LTE
					} else if maxValue != nil && (simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE) {
						minValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[0])
						defer storage.PutPooledColumnValue(minValue)
						includeMin = simpleExpr.Operator == storage.GTE
					}
				}
			}

			// Process range query
			if hasRange {
				// If no more conditions, directly use range query
				if len(columnConditions) <= 2 {
					idx.ForEachRowIDInRange(minValue, maxValue, includeMin, includeMax, callback)
					return
				}

				// Otherwise, do a single pass and collect candidates
				idx.mutex.RLock()

				candidateRows := GetCandidateRows()

				rowToKey := int64KeyMapPool.Get().(*fastmap.Int64Map[*MultiColumnKey])
				seen := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])

				// Find all rows in the range in a single traversal
				idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
					// Skip if the key doesn't have any columns
					if len(key.Values) == 0 {
						return true
					}

					// Check if the key's first column is in range
					inRange := true

					// Check min bound if specified
					if minValue != nil && !minValue.IsNull() {
						cmp, err := key.Values[0].Compare(minValue)
						if err != nil || cmp < 0 || (cmp == 0 && !includeMin) {
							inRange = false
						}
					}

					// Check max bound if specified
					if inRange && maxValue != nil && !maxValue.IsNull() {
						cmp, err := key.Values[0].Compare(maxValue)
						if err != nil || cmp > 0 || (cmp == 0 && !includeMax) {
							inRange = false
						}
					}

					// If in range, collect the rows
					if inRange {
						for _, rowID := range rowIDs {
							if alreadySeen := seen.Has(rowID); !alreadySeen {
								seen.Put(rowID, struct{}{})
								candidateRows = append(candidateRows, rowID)
								rowToKey.Put(rowID, key)
							}
						}
					}

					return true
				})

				idx.mutex.RUnlock()

				seen.Clear()
				int64StructMapPool.Put(seen)

				// Apply remaining conditions to the candidate rows
				for _, rowID := range candidateRows {
					if idx.matchesRemainingConditions(rowID, columnConditions, nil, rowToKey) {
						if !callback(rowID) {
							break
						}
					}
				}

				rowToKey.Clear()
				int64KeyMapPool.Put(rowToKey)

				PutCandidateRows(candidateRows)

				return
			}
		}
	}

	// For more complex cases, build a complete row-to-key mapping
	idx.mutex.RLock()

	// Build a mapping of all row IDs to their keys
	rowToKey := int64KeyMapPool.Get().(*fastmap.Int64Map[*MultiColumnKey])
	allRows := int64StructMapPool.Get().(*fastmap.Int64Map[struct{}])

	defer func() {
		rowToKey.Clear()
		int64KeyMapPool.Put(rowToKey)

		allRows.Clear()
		int64StructMapPool.Put(allRows)
	}()

	idx.valueTree.ForEach(func(key *MultiColumnKey, rowIDs []int64) bool {
		for _, rowID := range rowIDs {
			rowToKey.Put(rowID, key)
			allRows.Put(rowID, struct{}{})
		}
		return true
	})

	// Find the most selective column to start with
	var mostSelectiveCol string
	if _, hasFirst := columnConditions[idx.columnNames[0]]; hasFirst {
		mostSelectiveCol = idx.columnNames[0]
	} else {
		// Use any column we have
		for colName := range columnConditions {
			mostSelectiveCol = colName
			break
		}
	}

	// Get column index for most selective column
	colIdx := -1
	for i, colName := range idx.columnNames {
		if colName == mostSelectiveCol {
			colIdx = i
			break
		}
	}

	idx.mutex.RUnlock()

	// If we found a column to filter on
	if colIdx >= 0 {
		expr := columnConditions[mostSelectiveCol]

		// Create a predicate function for the selective column
		predicate := createPredicateForColumn(expr, idx.dataTypes[colIdx])

		// We'll create a slice of values that have already been applied
		var alreadyApplied []storage.ColumnValue
		if mostSelectiveCol == idx.columnNames[0] {
			value := storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[0])
			defer storage.PutPooledColumnValue(value)
			alreadyApplied = []storage.ColumnValue{value}
		}

		// Filter all rows by the most selective predicate first
		candidateRows := GetCandidateRows()
		for rowID := range allRows.Keys() {
			key, ok := rowToKey.Get(rowID)
			if !ok || colIdx >= len(key.Values) {
				continue
			}

			// Apply the selective predicate
			if predicate(key.Values[colIdx]) {
				candidateRows = append(candidateRows, rowID)
			}
		}

		// Apply remaining conditions to the candidate rows
		for _, rowID := range candidateRows {
			if idx.matchesRemainingConditions(rowID, columnConditions, alreadyApplied, rowToKey) {
				if !callback(rowID) {
					// Clean up resources to avoid leaks
					PutCandidateRows(candidateRows)
					break
				}
			}
		}

		// Clean up the candidates slice
		PutCandidateRows(candidateRows)
	}
}

// matchesRemainingConditions checks if a row matches the remaining conditions
// This optimized version avoids traversing the entire tree and instead
// uses a map lookup to get the values for a specific row
func (idx *MultiColumnarIndex) matchesRemainingConditions(
	rowID int64,
	conditions map[string]*expression.SimpleExpression,
	alreadyApplied []storage.ColumnValue,
	rowToKey *fastmap.Int64Map[*MultiColumnKey]) bool {

	// If no conditions to check, return true
	if len(conditions) == 0 {
		return true
	}

	// If we don't have the row's key, it can't match
	key, found := rowToKey.Get(rowID)
	if !found {
		return false
	}

	// Use a fixed-size array for predicates in common case (avoid allocation)
	// Most queries have less than 8 conditions, so this avoids heap allocations
	const maxPredicatesOnStack = 8
	type predicateInfo struct {
		colIdx   int
		expr     *expression.SimpleExpression
		dataType storage.DataType
	}

	// Use stack-allocated array for common case
	var predicatesOnStack [maxPredicatesOnStack]predicateInfo
	predicateCount := 0

	// Determine which columns have already been applied
	// Use a fixed-size array for common case (avoid allocation)
	// We rarely have more than 16 columns
	const maxAppliedCols = 16
	var appliedColsOnStack [maxAppliedCols]bool

	// Track applied columns as a bitmap
	if len(alreadyApplied) > 0 {
		// Initialize the 'bitmap' (array)
		for i := range alreadyApplied {
			if i < len(idx.columnNames) && i < maxAppliedCols {
				// Find the column name
				colName := idx.columnNames[i]

				// Find the index in our fixed array
				for j, name := range idx.columnNames {
					if name == colName && j < maxAppliedCols {
						appliedColsOnStack[j] = true
						break
					}
				}
			}
		}
	}

	// Build predicate info for remaining conditions (without creating the actual predicate functions yet)
	for colName, expr := range conditions {
		// Find column index
		colIdx := -1
		for i, name := range idx.columnNames {
			if name == colName {
				colIdx = i
				break
			}
		}

		// Skip if column not found or already applied
		if colIdx < 0 || (colIdx < maxAppliedCols && appliedColsOnStack[colIdx]) {
			continue
		}

		// Add predicate info to our array
		if predicateCount < maxPredicatesOnStack {
			predicatesOnStack[predicateCount] = predicateInfo{
				colIdx:   colIdx,
				expr:     expr,
				dataType: idx.dataTypes[colIdx],
			}
			predicateCount++
		} else {
			// Extremely rare case with many predicates
			// If we exceed stack capacity, we can't match efficiently
			// In practice, this should almost never happen
			return false
		}
	}

	// If no predicates to apply, return true
	if predicateCount == 0 {
		return true
	}

	// Apply direct comparisons to column values (avoiding predicate function creation)
	for i := 0; i < predicateCount; i++ {
		predInfo := predicatesOnStack[i]
		colIdx := predInfo.colIdx
		expr := predInfo.expr
		dataType := predInfo.dataType

		// Skip if column out of range
		if colIdx >= len(key.Values) {
			return false
		}

		// Get the column value from the key
		colValue := key.Values[colIdx]

		exprValue := storage.ValueToTypedValue(expr.Value, dataType)

		// Apply the operator directly based on type and operator
		// This avoids creating closures and improves performance
		switch expr.Operator {
		case storage.EQ:
			// Handle null cases
			if colValue == nil || colValue.IsNull() {
				if expr.Value == nil {
					continue // NULL = NULL is true
				}
				return false
			}

			// Type-specific equality check
			switch dataType {
			case storage.INTEGER:
				v1, ok1 := colValue.AsInt64()
				v2, ok2 := exprValue.(int64)
				if !ok1 || !ok2 || v1 != v2 {
					return false
				}
			case storage.FLOAT:
				v1, ok1 := colValue.AsFloat64()
				v2, ok2 := exprValue.(float64)
				if !ok1 || !ok2 || v1 != v2 {
					return false
				}
			case storage.TEXT, storage.JSON:
				v1, ok1 := colValue.AsString()
				v2, ok2 := exprValue.(string)
				if !ok1 || !ok2 || v1 != v2 {
					return false
				}
			case storage.BOOLEAN:
				v1, ok1 := colValue.AsBoolean()
				v2, ok2 := exprValue.(bool)
				if !ok1 || !ok2 || v1 != v2 {
					return false
				}
			case storage.TIMESTAMP:
				v1, ok1 := colValue.AsTimestamp()
				v2, ok2 := exprValue.(time.Time)
				if !ok1 || !ok2 || !v1.Equal(v2) {
					return false
				}
			default:
				return false
			}

		case storage.GT, storage.GTE, storage.LT, storage.LTE:
			// Handle null cases
			if colValue == nil || colValue.IsNull() {
				return false
			}

			// Get comparison result once
			var cmp int

			// Type-specific comparison for better performance
			switch dataType {
			case storage.INTEGER:
				v1, ok1 := colValue.AsInt64()
				v2, ok2 := exprValue.(int64)
				if !ok1 || !ok2 {
					return false
				}
				if v1 < v2 {
					cmp = -1
				} else if v1 > v2 {
					cmp = 1
				} else {
					cmp = 0
				}
			case storage.FLOAT:
				v1, ok1 := colValue.AsFloat64()
				v2, ok2 := exprValue.(float64)
				if !ok1 || !ok2 {
					return false
				}
				if v1 < v2 {
					cmp = -1
				} else if v1 > v2 {
					cmp = 1
				} else {
					cmp = 0
				}
			case storage.TEXT, storage.JSON:
				v1, ok1 := colValue.AsString()
				v2, ok2 := exprValue.(string)
				if !ok1 || !ok2 {
					return false
				}
				if v1 < v2 {
					cmp = -1
				} else if v1 > v2 {
					cmp = 1
				} else {
					cmp = 0
				}
			case storage.TIMESTAMP:
				v1, ok1 := colValue.AsTimestamp()
				v2, ok2 := exprValue.(time.Time)
				if !ok1 || !ok2 {
					return false
				}
				if v1.Before(v2) {
					cmp = -1
				} else if v1.After(v2) {
					cmp = 1
				} else {
					cmp = 0
				}
			default:
				return false // Unsupported type for comparison
			}

			// Check if comparison satisfies the operator
			switch expr.Operator {
			case storage.GT:
				if cmp <= 0 {
					return false
				}
			case storage.GTE:
				if cmp < 0 {
					return false
				}
			case storage.LT:
				if cmp >= 0 {
					return false
				}
			case storage.LTE:
				if cmp > 0 {
					return false
				}
			}

		case storage.ISNULL:
			if colValue != nil && !colValue.IsNull() {
				return false
			}

		case storage.ISNOTNULL:
			if colValue == nil || colValue.IsNull() {
				return false
			}

		default:
			// For other operators, fall back to using createPredicateForColumn
			predicate := createPredicateForColumn(expr, dataType)
			if !predicate(colValue) {
				return false
			}
		}
	}

	// All conditions matched
	return true
}

// FindWithOperator finds all row IDs that match the operation - implements IndexInterface
func (idx *MultiColumnarIndex) FindWithOperator(op storage.Operator, value storage.ColumnValue) ([]storage.IndexEntry, error) {
	var rowIDs []int64

	switch op {
	case storage.EQ:
		rowIDs = idx.GetRowIDsEqual(value)

	case storage.GT, storage.GTE, storage.LT, storage.LTE:
		var minValue, maxValue storage.ColumnValue
		var includeMin, includeMax bool

		if op == storage.GT || op == storage.GTE {
			minValue = value
			includeMin = op == storage.GTE
		} else {
			maxValue = value
			includeMax = op == storage.LTE
		}

		rowIDs = idx.GetRowIDsInRange(minValue, maxValue, includeMin, includeMax)

	case storage.ISNULL:
		idx.mutex.RLock()
		colID := idx.columnIDs[0]
		nullRows := idx.nullRowsByColumn[colID]
		if len(nullRows) > 0 {
			rowIDs = make([]int64, len(nullRows))
			copy(rowIDs, nullRows)
		}
		idx.mutex.RUnlock()

	case storage.ISNOTNULL:
		// Handle using the first column predicate function
		expr := &expression.SimpleExpression{
			Column:   idx.columnNames[0],
			Operator: storage.ISNOTNULL,
		}
		rowIDs = idx.handleFirstColumnPredicate(expr)
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

// IsUnique returns whether this index enforces uniqueness - implements IndexInterface
func (idx *MultiColumnarIndex) IsUnique() bool {
	return idx.isUnique
}

// Build builds or rebuilds the index from the version store
func (idx *MultiColumnarIndex) Build() error {
	// Clear existing data
	idx.mutex.Lock()

	// Clear multi-column data structures
	idx.valueTree = btree.NewBTree[*MultiColumnKey, []int64]()

	// Initialize null tracking for each column
	for colID := range idx.nullRowsByColumn {
		idx.nullRowsByColumn[colID] = make([]int64, 0, 16)
	}
	idx.mutex.Unlock()

	// Get schema to check if this is a primary key column
	if idx.versionStore != nil {
		if schema, err := idx.versionStore.GetTableSchema(); err == nil {
			// Check if any of our columns are part of the primary key
			for _, col := range schema.Columns {
				for _, colName := range idx.columnNames {
					if col.Name == colName && col.PrimaryKey {
						idx.isPrimaryKey = true
						break
					}
				}
				if idx.isPrimaryKey {
					break
				}
			}
		}
	}

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

		// Build values array for each of our columns
		values := make([]storage.ColumnValue, len(idx.columnIDs))

		for i, colID := range idx.columnIDs {
			// If column ID is out of range, treat as NULL
			if colID >= len(version.Data) {
				values[i] = nil
			} else {
				values[i] = version.Data[colID]
			}
		}

		// Add to index
		idx.AddMulti(values, rowID, 0)

		return true
	})

	return nil
}

// Close releases resources held by the index - implements Index interface
func (idx *MultiColumnarIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.valueTree = nil

	// Clear nullRows for each column
	for colID := range idx.nullRowsByColumn {
		idx.nullRowsByColumn[colID] = nil
	}

	return nil
}

var int64StructMapPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[struct{}](1000)
	},
}

var int64KeyMapPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[*MultiColumnKey](1000)
	},
}

var multiColumnKeyPool = sync.Pool{
	New: func() interface{} {
		return &MultiColumnKey{
			Values:    make([]storage.ColumnValue, 0, 8), // Initial capacity of 8
			DataTypes: make([]storage.DataType, 0, 8),
		}
	},
}

// Define a type and pool for storing candidate row IDs
var candidateRowsPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate with a reasonable capacity for common queries
		return make([]int64, 0, 1024)
	},
}

// GetCandidateRows gets a slice of int64 from the pool
func GetCandidateRows() []int64 {
	candidates := candidateRowsPool.Get().([]int64)
	// Reset length but preserve capacity
	return candidates[:0]
}

// PutCandidateRows returns a slice to the pool
func PutCandidateRows(candidates []int64) {
	// No need to clear since we'll reset length on Get
	candidateRowsPool.Put(candidates)
}

// GetMultiColumnKey gets a MultiColumnKey from the pool or creates a new one
func GetMultiColumnKey(capacity int) *MultiColumnKey {
	key := multiColumnKeyPool.Get().(*MultiColumnKey)
	// Ensure capacity
	if cap(key.Values) < capacity {
		key.Values = make([]storage.ColumnValue, 0, capacity)
		key.DataTypes = make([]storage.DataType, 0, capacity)
	}
	// Reset lengths
	key.Values = key.Values[:0]
	key.DataTypes = key.DataTypes[:0]
	return key
}

// PutMultiColumnKey returns a MultiColumnKey to the pool
func PutMultiColumnKey(key *MultiColumnKey) {
	// Clear references to help GC
	for i := range key.Values {
		key.Values[i] = nil
	}
	multiColumnKeyPool.Put(key)
}

// PredicateFunc defines the signature for predicate functions
type PredicateFunc func(storage.ColumnValue) bool

// PredicateFactory contains cached predicates by type and value to reduce allocations
type PredicateFactory struct {
	// Special case static predicates that don't need expression values
	isNullPredicate    PredicateFunc
	isNotNullPredicate PredicateFunc
}

// Single global instance of PredicateFactory to avoid allocations
var predicateFactory = &PredicateFactory{
	// Initialize static predicates once
	isNullPredicate: func(value storage.ColumnValue) bool {
		return value == nil || value.IsNull()
	},
	isNotNullPredicate: func(value storage.ColumnValue) bool {
		return value != nil && !value.IsNull()
	},
}

// createPredicateForColumn creates an efficient predicate function for a column
// This optimized version avoids creating new closures when possible
func createPredicateForColumn(expr *expression.SimpleExpression, dataType storage.DataType) PredicateFunc {
	// Special case for NULL checks - use static predicates
	if expr.Operator == storage.ISNULL {
		return predicateFactory.isNullPredicate
	}

	if expr.Operator == storage.ISNOTNULL {
		return predicateFactory.isNotNullPredicate
	}

	// For comparison operators, we need to capture expression value,
	// so we still need to create closures, but we'll optimize them

	// Convert expression value to ColumnValue once
	exprValue := storage.ValueToPooledColumnValue(expr.Value, dataType)
	defer storage.PutPooledColumnValue(exprValue)

	// Create predicate based on operator and type
	var predicate PredicateFunc

	switch expr.Operator {
	case storage.EQ:
		// Type-specific equality check
		switch dataType {
		case storage.INTEGER:
			// For integer values, optimize the common case
			if intVal, ok := exprValue.AsInt64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsInt64()
					return ok && v == intVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return exprValue == nil || exprValue.IsNull()
					}
					v1, ok1 := value.AsInt64()
					v2, ok2 := exprValue.AsInt64()
					return ok1 && ok2 && v1 == v2
				}
			}

		case storage.FLOAT:
			if floatVal, ok := exprValue.AsFloat64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsFloat64()
					return ok && v == floatVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return exprValue == nil || exprValue.IsNull()
					}
					v1, ok1 := value.AsFloat64()
					v2, ok2 := exprValue.AsFloat64()
					return ok1 && ok2 && v1 == v2
				}
			}

		case storage.TEXT, storage.JSON:
			if strVal, ok := exprValue.AsString(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsString()
					return ok && v == strVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return exprValue == nil || exprValue.IsNull()
					}
					v1, ok1 := value.AsString()
					v2, ok2 := exprValue.AsString()
					return ok1 && ok2 && v1 == v2
				}
			}

		case storage.BOOLEAN:
			if boolVal, ok := exprValue.AsBoolean(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsBoolean()
					return ok && v == boolVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return exprValue == nil || exprValue.IsNull()
					}
					v1, ok1 := value.AsBoolean()
					v2, ok2 := exprValue.AsBoolean()
					return ok1 && ok2 && v1 == v2
				}
			}

		case storage.TIMESTAMP:
			if timeVal, ok := exprValue.AsTimestamp(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsTimestamp()
					return ok && v.Equal(timeVal)
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return exprValue == nil || exprValue.IsNull()
					}
					v1, ok1 := value.AsTimestamp()
					v2, ok2 := exprValue.AsTimestamp()
					return ok1 && ok2 && v1.Equal(v2)
				}
			}

		default:
			// Fallback to general comparison
			predicate = func(value storage.ColumnValue) bool {
				if value == nil || value.IsNull() {
					return exprValue == nil || exprValue.IsNull()
				}
				cmp, err := value.Compare(exprValue)
				return err == nil && cmp == 0
			}
		}

	case storage.GT:
		// Optimize for INTEGER which is the most common case
		if dataType == storage.INTEGER {
			if intVal, ok := exprValue.AsInt64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsInt64()
					return ok && v > intVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v1, ok1 := value.AsInt64()
					v2, ok2 := exprValue.AsInt64()
					return ok1 && ok2 && v1 > v2
				}
			}
		} else {
			// For other types, create a standard comparison predicate
			predicate = func(value storage.ColumnValue) bool {
				if value == nil || value.IsNull() {
					return false
				}
				cmp, err := value.Compare(exprValue)
				return err == nil && cmp > 0
			}
		}

	case storage.GTE:
		// Optimize for INTEGER which is the most common case
		if dataType == storage.INTEGER {
			if intVal, ok := exprValue.AsInt64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsInt64()
					return ok && v >= intVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v1, ok1 := value.AsInt64()
					v2, ok2 := exprValue.AsInt64()
					return ok1 && ok2 && v1 >= v2
				}
			}
		} else {
			// For other types, create a standard comparison predicate
			predicate = func(value storage.ColumnValue) bool {
				if value == nil || value.IsNull() {
					return false
				}
				cmp, err := value.Compare(exprValue)
				return err == nil && cmp >= 0
			}
		}

	case storage.LT:
		// Optimize for INTEGER which is the most common case
		if dataType == storage.INTEGER {
			if intVal, ok := exprValue.AsInt64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsInt64()
					return ok && v < intVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v1, ok1 := value.AsInt64()
					v2, ok2 := exprValue.AsInt64()
					return ok1 && ok2 && v1 < v2
				}
			}
		} else {
			// For other types, create a standard comparison predicate
			predicate = func(value storage.ColumnValue) bool {
				if value == nil || value.IsNull() {
					return false
				}
				cmp, err := value.Compare(exprValue)
				return err == nil && cmp < 0
			}
		}

	case storage.LTE:
		// Optimize for INTEGER which is the most common case
		if dataType == storage.INTEGER {
			if intVal, ok := exprValue.AsInt64(); ok {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v, ok := value.AsInt64()
					return ok && v <= intVal
				}
			} else {
				predicate = func(value storage.ColumnValue) bool {
					if value == nil || value.IsNull() {
						return false
					}
					v1, ok1 := value.AsInt64()
					v2, ok2 := exprValue.AsInt64()
					return ok1 && ok2 && v1 <= v2
				}
			}
		} else {
			// For other types, create a standard comparison predicate
			predicate = func(value storage.ColumnValue) bool {
				if value == nil || value.IsNull() {
					return false
				}
				cmp, err := value.Compare(exprValue)
				return err == nil && cmp <= 0
			}
		}

	default:
		// Default predicate that never matches
		predicate = func(value storage.ColumnValue) bool {
			_ = value
			return false
		}
	}

	// Cleanup the pooled expression value when the predicate is no longer needed
	// This is done by creating a wrapper function that holds ownership of exprValue
	return func(value storage.ColumnValue) bool {
		result := predicate(value)
		// We can't put the value back here as we don't know when the last use is
		// The caller should handle this with storage.PutPooledColumnValue
		return result
	}
}
