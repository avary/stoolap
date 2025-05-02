// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"sort"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// Ordering function type for comparing values
type compareFunc func(a, b interface{}) int

// btreeNode represents a node in the B-tree
type btreeNode struct {
	keys     []interface{} // Keys stored in this node
	rowIDs   [][]int64     // Row IDs for each key
	children []*btreeNode  // Child nodes (nil for leaf nodes)
	isLeaf   bool          // Whether this is a leaf node
	compare  compareFunc   // Function for comparing keys
}

// Find returns the index where the key should be inserted
func (n *btreeNode) findIndex(key interface{}) int {
	// Binary search for the key
	return sort.Search(len(n.keys), func(i int) bool {
		return n.compare(n.keys[i], key) >= 0
	})
}

// insert inserts a key and rowID into the B-tree node
func (n *btreeNode) insert(key interface{}, rowID int64) {
	i := n.findIndex(key)

	// If key already exists, just append the rowID
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		n.rowIDs[i] = append(n.rowIDs[i], rowID)
		return
	}

	// Insert new key and rowID
	n.keys = append(n.keys, nil)
	n.rowIDs = append(n.rowIDs, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.rowIDs[i+1:], n.rowIDs[i:])
	n.keys[i] = key
	n.rowIDs[i] = []int64{rowID}
}

// remove removes a rowID from a key in the B-tree node
func (n *btreeNode) remove(key interface{}, rowID int64) bool {
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

// rangeSearch finds all rowIDs in a range [min, max]
func (n *btreeNode) rangeSearch(min, max interface{}, includeMin, includeMax bool, result *[]int64) {
	if len(n.keys) == 0 {
		return
	}

	// Find the start index
	startIdx := 0
	if min != nil {
		startIdx = sort.Search(len(n.keys), func(i int) bool {
			if includeMin {
				return n.compare(n.keys[i], min) >= 0
			}
			return n.compare(n.keys[i], min) > 0
		})
	}

	// Find the end index
	endIdx := len(n.keys)
	if max != nil {
		endIdx = sort.Search(len(n.keys), func(i int) bool {
			if includeMax {
				return n.compare(n.keys[i], max) > 0
			}
			return n.compare(n.keys[i], max) >= 0
		})
	}

	// Collect all rowIDs in the range
	for i := startIdx; i < endIdx; i++ {
		*result = append(*result, n.rowIDs[i]...)
	}
}

// equalSearch finds all rowIDs with the given key
func (n *btreeNode) equalSearch(key interface{}, result *[]int64) {
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
func (n *btreeNode) getAll(result *[]int64) {
	for _, ids := range n.rowIDs {
		*result = append(*result, ids...)
	}
}

// btree implements a B-tree data structure optimized for columnar indexes
type btreeColumnar struct {
	root    *btreeNode  // Root node of the B-tree
	compare compareFunc // Function for comparing keys
	size    int         // Number of keys in the tree
}

// newBTree creates a new B-tree with the given compare function
func newBTree(compare compareFunc) *btreeColumnar {
	return &btreeColumnar{
		root: &btreeNode{
			keys:     make([]interface{}, 0),
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
func (t *btreeColumnar) Insert(key interface{}, rowID int64) {
	t.root.insert(key, rowID)
	t.size++
}

// Remove removes a key and rowID from the B-tree
func (t *btreeColumnar) Remove(key interface{}, rowID int64) bool {
	result := t.root.remove(key, rowID)
	if result {
		t.size--
	}
	return result
}

// ValueCount returns the number of occurrences of a key
func (t *btreeColumnar) ValueCount(key interface{}) int {
	i := t.root.findIndex(key)
	if i < len(t.root.keys) && t.compare(t.root.keys[i], key) == 0 {
		return len(t.root.rowIDs[i])
	}
	return 0
}

// RangeSearch finds all rowIDs in a range [min, max]
func (t *btreeColumnar) RangeSearch(min, max interface{}, includeMin, includeMax bool) []int64 {
	result := make([]int64, 0, 100) // Start with a reasonable capacity
	t.root.rangeSearch(min, max, includeMin, includeMax, &result)
	return result
}

// EqualSearch finds all rowIDs with the given key
func (t *btreeColumnar) EqualSearch(key interface{}) []int64 {
	result := make([]int64, 0, 10) // Start with a reasonable capacity
	t.root.equalSearch(key, &result)
	return result
}

// GetAll returns all rowIDs in the B-tree
func (t *btreeColumnar) GetAll() []int64 {
	result := make([]int64, 0, t.size)
	t.root.getAll(&result)
	return result
}

// Size returns the number of keys in the B-tree
func (t *btreeColumnar) Size() int {
	return t.size
}

// Clear removes all entries from the B-tree
func (t *btreeColumnar) Clear() {
	t.root = &btreeNode{
		keys:     make([]interface{}, 0),
		rowIDs:   make([][]int64, 0),
		children: nil,
		isLeaf:   true,
		compare:  t.compare,
	}
	t.size = 0
}

// compareTimestamps provides enhanced timestamp comparison for optimized timestamp operations
func compareTimestamps(a, b time.Time, dataType storage.DataType) int {
	// First normalize based on data type to ensure proper comparison
	aNorm := storage.NormalizeForDataType(a, dataType)
	bNorm := storage.NormalizeForDataType(b, dataType)

	// For pure DATE comparisons, we want to compare only the date part
	if dataType == storage.DATE {
		return aNorm.Compare(bNorm)
	}

	// For TIMESTAMP comparisons, we use Unix time for better performance
	aUnix := aNorm.UnixNano()
	bUnix := bNorm.UnixNano()

	if aUnix < bUnix {
		return -1
	}
	if aUnix > bUnix {
		return 1
	}
	return 0
}

// compareColumnValues compares two ColumnValue objects
// This is the key function that allows us to use a single B-tree for all data types
func compareColumnValues(a, b interface{}) int {
	aVal, aOK := a.(storage.ColumnValue)
	bVal, bOK := b.(storage.ColumnValue)

	if !aOK || !bOK {
		return 0
	}

	// Handle NULL values
	if aVal.IsNull() && bVal.IsNull() {
		return 0
	}
	if aVal.IsNull() {
		return -1
	}
	if bVal.IsNull() {
		return 1
	}

	// Make sure both values have the same type
	if aVal.Type() != bVal.Type() {
		// Sort by type ID if different types (shouldn't normally happen)
		if int(aVal.Type()) < int(bVal.Type()) {
			return -1
		}
		return 1
	}

	// Compare based on data type
	switch aVal.Type() {
	case storage.INTEGER:
		aInt, _ := aVal.AsInt64()
		bInt, _ := bVal.AsInt64()
		if aInt < bInt {
			return -1
		}
		if aInt > bInt {
			return 1
		}
		return 0

	case storage.FLOAT:
		aFloat, _ := aVal.AsFloat64()
		bFloat, _ := bVal.AsFloat64()
		if aFloat < bFloat {
			return -1
		}
		if aFloat > bFloat {
			return 1
		}
		return 0

	case storage.TEXT, storage.JSON:
		aStr, _ := aVal.AsString()
		bStr, _ := bVal.AsString()
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0

	case storage.BOOLEAN:
		aBool, _ := aVal.AsBoolean()
		bBool, _ := bVal.AsBoolean()
		if aBool == bBool {
			return 0
		}
		if !aBool && bBool {
			return -1
		}
		return 1

	case storage.TIMESTAMP, storage.DATE, storage.TIME:
		aTime, _ := aVal.AsTimestamp()
		bTime, _ := bVal.AsTimestamp()
		// Use specialized timestamp comparison for better performance
		return compareTimestamps(aTime, bTime, aVal.Type())
	}

	return 0
}

// Note: This function was removed as it's not needed in the current implementation.

// TimeBucketGranularity represents a time bucketing granularity
type TimeBucketGranularity int

const (
	// HourBucket buckets timestamps by hour
	HourBucket TimeBucketGranularity = iota
	// DayBucket buckets timestamps by day
	DayBucket
	// WeekBucket buckets timestamps by week
	WeekBucket
	// MonthBucket buckets timestamps by month
	MonthBucket
	// YearBucket buckets timestamps by year
	YearBucket
)

// ColumnarIndex represents a column-oriented index optimized for range queries
// using a B-tree data structure for efficient lookups and range scans
type ColumnarIndex struct {
	// name is the index name
	name string

	// columnName is the name of the column this index is for
	columnName string

	// columnID is the position of the column in the schema
	columnID int

	// dataType is the type of the column
	dataType storage.DataType

	// Single B-tree that works directly with ColumnValue objects
	// This eliminates unnecessary conversions between types
	valueTree *btreeColumnar

	// nullRows tracks rows with NULL in this column
	nullRows []int64

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

// NewColumnarIndex creates a new ColumnarIndex
func NewColumnarIndex(name, tableName, columnName string,
	columnID int, dataType storage.DataType,
	versionStore *VersionStore, isUnique bool) *ColumnarIndex {

	idx := &ColumnarIndex{
		name:                  name,
		columnName:            columnName,
		columnID:              columnID,
		dataType:              dataType,
		valueTree:             newBTree(compareColumnValues), // Use single tree with ColumnValue comparator
		nullRows:              make([]int64, 0, 16),
		mutex:                 sync.RWMutex{},
		tableName:             tableName,
		versionStore:          versionStore,
		isUnique:              isUnique,
		isPrimaryKey:          false,     // Will be set during Build based on schema
		timeBucketGranularity: DayBucket, // Default to day bucketing
		enableTimeBucketing:   false,     // Disabled by default
	}

	// Auto-enable time bucketing for timestamp columns
	if dataType == storage.TIMESTAMP {
		idx.enableTimeBucketing = true
	}

	return idx
}

// EnableTimeBucketing enables time bucketing for timestamp columns
func (idx *ColumnarIndex) EnableTimeBucketing(granularity TimeBucketGranularity) {
	// Only enable for timestamp columns
	if idx.dataType != storage.TIMESTAMP {
		return
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.timeBucketGranularity = granularity
	idx.enableTimeBucketing = true
}

// DisableTimeBucketing disables time bucketing
func (idx *ColumnarIndex) DisableTimeBucketing() {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.enableTimeBucketing = false
}

// Name returns the index name - implements IndexInterface
func (idx *ColumnarIndex) Name() string {
	return idx.name
}

// IndexType returns the type of the index - implements IndexInterface
func (idx *ColumnarIndex) IndexType() storage.IndexType {
	return storage.ColumnarIndex
}

// ColumnNames returns the names of the columns this index is for - implements IndexInterface
func (idx *ColumnarIndex) ColumnNames() []string {
	return []string{idx.columnName}
}

// ColumnID returns the column ID for this index
func (idx *ColumnarIndex) ColumnID() int {
	return idx.columnID
}

// HasUniqueValue checks if a value already exists in a unique index
// This is a fast path check that doesn't modify the index
func (idx *ColumnarIndex) HasUniqueValue(value storage.ColumnValue) bool {
	// Skip NULL values - they don't violate uniqueness
	if value == nil || value.IsNull() {
		return false
	}

	// Only do this check for unique indexes
	if !idx.isUnique {
		return false
	}

	// Use a read lock since we're only reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Fast path: Just check if the value exists in the B-tree
	count := idx.valueTree.ValueCount(value)

	return count > 0
}

// Add adds a value to the index with the given row ID - implements IndexInterface
func (idx *ColumnarIndex) Add(value storage.ColumnValue, rowID int64, refID int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Handle NULL value - NULLs are always allowed, even with unique constraint
	if value == nil || value.IsNull() {
		idx.nullRows = append(idx.nullRows, rowID)
		return nil
	}

	// Fast path for non-unique indexes (most common case)
	if !idx.isUnique {
		// Store the ColumnValue directly in the B-tree
		idx.valueTree.Insert(value, rowID)
		return nil
	}

	// For unique indexes, check if the value already exists
	// We don't need to allocate a new slice for this check - just peek
	count := idx.valueTree.ValueCount(value)
	if count > 0 {
		// Return unique constraint violation
		return storage.ErrUniqueConstraintViolation
	}

	// If we get here, the value is unique, so insert it
	idx.valueTree.Insert(value, rowID)
	return nil
}

// AddMulti adds multiple values to the index for multi-column indexes - implements IndexInterface
func (idx *ColumnarIndex) AddMulti(values []storage.ColumnValue, rowID int64, refID int64) error {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return nil
	}
	return idx.Add(values[0], rowID, refID)
}

// Find finds all pairs where the column equals the given value - implements Index interface
func (idx *ColumnarIndex) Find(value storage.ColumnValue) ([]storage.IndexEntry, error) {
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

// GetRowIDsEqual returns row IDs with the given value
func (idx *ColumnarIndex) GetRowIDsEqual(value storage.ColumnValue) []int64 {
	// Fast path for common equality checks
	if value == nil || value.IsNull() {
		// NULL value check
		idx.mutex.RLock()

		// Quick return for empty nullRows
		if len(idx.nullRows) == 0 {
			idx.mutex.RUnlock()
			return nil
		}

		// Return a copy of nullRows
		result := make([]int64, len(idx.nullRows))
		copy(result, idx.nullRows)
		idx.mutex.RUnlock()
		return result
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Use the value directly in the B-tree
	// No conversion needed - more efficient
	return idx.valueTree.EqualSearch(value)
}

// FindRange finds all row IDs where the column is in the given range
func (idx *ColumnarIndex) FindRange(minValue, maxValue storage.ColumnValue,
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

// GetRowIDsInRange returns row IDs with values in the given range
func (idx *ColumnarIndex) GetRowIDsInRange(minValue, maxValue storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Fast path - most common case
	if !(idx.dataType == storage.TIMESTAMP || idx.dataType == storage.DATE) ||
		!(maxValue != nil && !maxValue.IsNull() && includeMax) {
		// Use the valueTree directly with ColumnValue objects for best performance
		return idx.valueTree.RangeSearch(minValue, maxValue, includeMin, includeMax)
	}

	// Special handling only for DATE type with inclusive upper bound - critical optimization path
	if idx.dataType == storage.DATE {
		// Make a copy of the value to avoid modifying the original
		if t, ok := maxValue.AsTimestamp(); ok {
			// Set to end of day to include the entire day
			t = storage.DateToEndOfDay(t)
			adjustedMaxValue := storage.NewDateValue(t)
			return idx.valueTree.RangeSearch(minValue, adjustedMaxValue, includeMin, includeMax)
		}
	}

	// Use the valueTree directly with ColumnValue objects
	return idx.valueTree.RangeSearch(minValue, maxValue, includeMin, includeMax)
}

// GetLatestBefore finds the most recent row IDs before a given timestamp
func (idx *ColumnarIndex) GetLatestBefore(timestamp time.Time) []int64 {
	if idx.dataType != storage.TIMESTAMP && idx.dataType != storage.DATE {
		return nil
	}

	// Convert to appropriate column value
	var tsValue storage.ColumnValue
	if idx.dataType == storage.TIMESTAMP {
		tsValue = storage.NewTimestampValue(timestamp)
	} else {
		tsValue = storage.NewDateValue(timestamp)
	}

	// Use range query with max value as the timestamp and no min value
	return idx.GetRowIDsInRange(nil, tsValue, false, true) // Up to and including timestamp
}

// GetRecentTimeRange finds row IDs within a recent time window (e.g., last hour, day)
func (idx *ColumnarIndex) GetRecentTimeRange(duration time.Duration) []int64 {
	if idx.dataType != storage.TIMESTAMP {
		return nil
	}

	now := time.Now()
	startTime := now.Add(-duration)

	// Convert to timestamp values
	startValue := storage.NewTimestampValue(startTime)
	endValue := storage.NewTimestampValue(now)

	// Get rows in the recent time range
	return idx.GetRowIDsInRange(startValue, endValue, true, true)
}

// Remove removes a value from the index - implements Index interface
func (idx *ColumnarIndex) Remove(value storage.ColumnValue, rowID int64, refID int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Handle NULL value
	if value == nil || value.IsNull() {
		for i, id := range idx.nullRows {
			if id == rowID {
				// Remove by swapping with the last element and truncating
				idx.nullRows[i] = idx.nullRows[len(idx.nullRows)-1]
				idx.nullRows = idx.nullRows[:len(idx.nullRows)-1]
				break
			}
		}
		return nil
	}

	// Remove directly from the valueTree
	idx.valueTree.Remove(value, rowID)

	return nil
}

// GetFilteredRowIDs returns row IDs that match the given expression
func (idx *ColumnarIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	if expr == nil {
		return nil
	}

	// Fast path for SimpleExpression with equality or NULL checks (most common cases)
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Only handle expressions for this column
		if simpleExpr.Column == idx.columnName {
			// Handle specific operators
			switch simpleExpr.Operator {
			case storage.EQ:
				// Convert the expression value to a ColumnValue with the correct type
				valueCol := storage.ValueToColumnValue(simpleExpr.Value, idx.dataType)
				return idx.GetRowIDsEqual(valueCol)

			case storage.GT, storage.GTE, storage.LT, storage.LTE:
				// Range query - simple inequality
				var minValue, maxValue storage.ColumnValue
				var includeMin, includeMax bool

				if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
					minValue = storage.ValueToColumnValue(simpleExpr.Value, idx.dataType)
					includeMin = simpleExpr.Operator == storage.GTE
				} else {
					maxValue = storage.ValueToColumnValue(simpleExpr.Value, idx.dataType)
					includeMax = simpleExpr.Operator == storage.LTE
				}

				return idx.GetRowIDsInRange(minValue, maxValue, includeMin, includeMax)

			case storage.ISNULL:
				// NULL check
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()
				if len(idx.nullRows) == 0 {
					return nil
				}
				result := make([]int64, len(idx.nullRows))
				copy(result, idx.nullRows)
				return result

			case storage.ISNOTNULL:
				// NOT NULL check - get all non-NULL values
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()
				return idx.valueTree.GetAll()
			}
		}
	}

	// Also check for AndExpression with range conditions
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		// Check if both expressions are for this column and represent a range
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		if ok1 && ok2 && expr1.Column == idx.columnName && expr2.Column == idx.columnName {
			// Fast path for range queries
			// Check for range patterns like (col > X AND col < Y)
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool
			var hasRange bool

			// Check if expr1 is a lower bound and expr2 is an upper bound
			if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
				(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {
				minValue = storage.ValueToColumnValue(expr1.Value, idx.dataType)
				includeMin = expr1.Operator == storage.GTE
				maxValue = storage.ValueToColumnValue(expr2.Value, idx.dataType)
				includeMax = expr2.Operator == storage.LTE
				hasRange = true
			}

			// Check if expr2 is a lower bound and expr1 is an upper bound
			if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
				(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {
				minValue = storage.ValueToColumnValue(expr2.Value, idx.dataType)
				includeMin = expr2.Operator == storage.GTE
				maxValue = storage.ValueToColumnValue(expr1.Value, idx.dataType)
				includeMax = expr1.Operator == storage.LTE
				hasRange = true
			}

			if hasRange {
				// Special case for DATE type with inclusive upper bound
				if idx.dataType == storage.DATE && maxValue != nil && !maxValue.IsNull() && includeMax {
					// Make a copy of the value to avoid modifying the original
					if t, ok := maxValue.AsTimestamp(); ok {
						// Set to end of day to include the entire day
						t = storage.DateToEndOfDay(t)
						maxValue = storage.NewDateValue(t)
					}
				}

				return idx.GetRowIDsInRange(minValue, maxValue, includeMin, includeMax)
			}
		}
	}

	// Special case for SchemaAwareExpression
	if schemaExpr, ok := expr.(*expression.SchemaAwareExpression); ok {
		// Try with the wrapped expression instead
		if schemaExpr.Expr != nil {
			return idx.GetFilteredRowIDs(schemaExpr.Expr)
		}
	}

	// For other cases, return nil to indicate we can't use this index
	return nil
}

// Build builds or rebuilds the index from the version store
func (idx *ColumnarIndex) Build() error {
	// Clear existing data
	idx.mutex.Lock()

	// Clear B-tree and nullRows
	if idx.valueTree != nil {
		idx.valueTree.Clear()
	} else {
		idx.valueTree = newBTree(compareColumnValues)
	}

	idx.nullRows = make([]int64, 0)
	idx.mutex.Unlock()

	// Get schema to check if this is a primary key column
	if idx.versionStore != nil {
		if schema, err := idx.versionStore.GetTableSchema(); err == nil {
			for _, col := range schema.Columns {
				if col.Name == idx.columnName && col.PrimaryKey {
					idx.isPrimaryKey = true
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
	for rowID, version := range visibleVersions {
		if version.IsDeleted {
			continue
		}

		// Get the value at the specified column ID
		if int(idx.columnID) < len(version.Data) {
			value := version.Data[idx.columnID]

			// Add to index
			idx.Add(value, rowID, 0)
		} else {
			// Column doesn't exist in this row, treat as NULL
			idx.Add(nil, rowID, 0)
		}
	}

	return nil
}

// FindWithOperator finds all row IDs that match the operation - implements IndexInterface
func (idx *ColumnarIndex) FindWithOperator(op storage.Operator, value storage.ColumnValue) ([]storage.IndexEntry, error) {
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
		if len(idx.nullRows) > 0 {
			rowIDs = make([]int64, len(idx.nullRows))
			copy(rowIDs, idx.nullRows)
		}
		idx.mutex.RUnlock()

	case storage.ISNOTNULL:
		idx.mutex.RLock()
		rowIDs = idx.valueTree.GetAll()
		idx.mutex.RUnlock()
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

func (idx *ColumnarIndex) IsUnique() bool {
	return idx.isUnique
}

// Close releases resources held by the index - implements Index interface
func (idx *ColumnarIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Clear valueTree and nullRows to free memory
	if idx.valueTree != nil {
		idx.valueTree.Clear()
		idx.valueTree = nil
	}

	idx.nullRows = nil

	return nil
}
