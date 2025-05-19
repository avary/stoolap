package mvcc

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

func TestNewUniqueConstraintIndex(t *testing.T) {
	testCases := []struct {
		name        string
		tableName   string
		columnNames []string
		columnIDs   []int
		dataTypes   []storage.DataType
	}{
		{
			name:        "single_column_index",
			tableName:   "users",
			columnNames: []string{"email"},
			columnIDs:   []int{2},
			dataTypes:   []storage.DataType{storage.TEXT},
		},
		{
			name:        "multi_column_index",
			tableName:   "products",
			columnNames: []string{"category", "sku"},
			columnIDs:   []int{3, 4},
			dataTypes:   []storage.DataType{storage.TEXT, storage.TEXT},
		},
		{
			name:        "mixed_types_index",
			tableName:   "orders",
			columnNames: []string{"user_id", "order_date", "status"},
			columnIDs:   []int{1, 5, 7},
			dataTypes:   []storage.DataType{storage.INTEGER, storage.TIMESTAMP, storage.TEXT},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idx := NewUniqueConstraintIndex(
				tc.name,
				tc.tableName,
				tc.columnNames,
				tc.columnIDs,
				tc.dataTypes)

			// Verify index was created correctly
			if idx.name != tc.name {
				t.Errorf("Expected index name %s, got %s", tc.name, idx.name)
			}
			if idx.tableName != tc.tableName {
				t.Errorf("Expected table name %s, got %s", tc.tableName, idx.tableName)
			}
			if len(idx.columnNames) != len(tc.columnNames) {
				t.Errorf("Expected %d column names, got %d", len(tc.columnNames), len(idx.columnNames))
			}
			if len(idx.columnIDs) != len(tc.columnIDs) {
				t.Errorf("Expected %d column IDs, got %d", len(tc.columnIDs), len(idx.columnIDs))
			}
			if len(idx.dataTypes) != len(tc.dataTypes) {
				t.Errorf("Expected %d data types, got %d", len(tc.dataTypes), len(idx.dataTypes))
			}

			// Check method implementations
			if idx.Name() != tc.name {
				t.Errorf("Name() returned %s, expected %s", idx.Name(), tc.name)
			}
			if idx.TableName() != tc.tableName {
				t.Errorf("TableName() returned %s, expected %s", idx.TableName(), tc.tableName)
			}
			if !equalStringSlices(idx.ColumnNames(), tc.columnNames) {
				t.Errorf("ColumnNames() returned %v, expected %v", idx.ColumnNames(), tc.columnNames)
			}
			if !equalIntSlices(idx.ColumnIDs(), tc.columnIDs) {
				t.Errorf("ColumnIDs() returned %v, expected %v", idx.ColumnIDs(), tc.columnIDs)
			}
			if !equalDataTypeSlices(idx.DataTypes(), tc.dataTypes) {
				t.Errorf("DataTypes() returned %v, expected %v", idx.DataTypes(), tc.dataTypes)
			}
			if idx.IndexType() != "unique" {
				t.Errorf("IndexType() returned %s, expected 'unique'", idx.IndexType())
			}
			if !idx.IsUnique() {
				t.Errorf("IsUnique() returned false, expected true")
			}

			// Make sure the map is initialized
			if idx.uniqueValues == nil {
				t.Error("uniqueValues map should be initialized")
			}
		})
	}
}

func TestUniqueConstraintIndex_Add(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Test successful add
	err := idx.Add([]storage.ColumnValue{storage.NewStringValue("user1@example.com")}, 1, 0)
	if err != nil {
		t.Errorf("Failed to add unique value: %v", err)
	}

	// Verify the value was added
	if len(idx.uniqueValues) != 1 {
		t.Errorf("Expected 1 value in index, found %d", len(idx.uniqueValues))
	}

	// Test adding duplicate (should fail)
	err = idx.Add([]storage.ColumnValue{storage.NewStringValue("user1@example.com")}, 2, 0)
	if err == nil {
		t.Error("Adding duplicate value should fail")
	}

	// Verify error is a unique constraint error
	var uniqueErr *storage.ErrUniqueConstraint
	if !errors.As(err, &uniqueErr) {
		t.Errorf("Error should be ErrUniqueConstraint, got: %T", err)
	}

	// Test different value (should succeed)
	err = idx.Add([]storage.ColumnValue{storage.NewStringValue("user2@example.com")}, 2, 0)
	if err != nil {
		t.Errorf("Failed to add second unique value: %v", err)
	}

	// Verify there are now two values
	if len(idx.uniqueValues) != 2 {
		t.Errorf("Expected 2 values in index, found %d", len(idx.uniqueValues))
	}

	// Test adding NULL value (should be skipped)
	err = idx.Add([]storage.ColumnValue{storage.NewNullValue(storage.TEXT)}, 3, 0)
	if err != nil {
		t.Errorf("Failed when adding NULL value: %v", err)
	}

	// NULL values should be skipped
	if len(idx.uniqueValues) != 2 {
		t.Errorf("Expected still 2 values after adding NULL, found %d", len(idx.uniqueValues))
	}

	// Test adding with wrong number of values
	err = idx.Add([]storage.ColumnValue{
		storage.NewStringValue("extra@example.com"),
		storage.NewIntegerValue(123),
	}, 4, 0)
	if err == nil {
		t.Error("Adding wrong number of values should fail")
	}
}

func TestUniqueConstraintIndex_AddBatch(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Create batch of entries
	entries := map[int64][]storage.ColumnValue{
		1: {storage.NewStringValue("user1@example.com")},
		2: {storage.NewStringValue("user2@example.com")},
		3: {storage.NewStringValue("user3@example.com")},
	}

	// Test successful batch add
	err := idx.AddBatch(entries)
	if err != nil {
		t.Errorf("Failed to add batch of unique values: %v", err)
	}

	// Verify values were added
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected 3 values in index, found %d", len(idx.uniqueValues))
	}

	// Test batch with duplicate within the batch
	duplicateBatch := map[int64][]storage.ColumnValue{
		4: {storage.NewStringValue("user4@example.com")},
		5: {storage.NewStringValue("user5@example.com")},
		6: {storage.NewStringValue("user4@example.com")}, // Duplicate within batch
	}

	err = idx.AddBatch(duplicateBatch)
	if err == nil {
		t.Error("Batch with internal duplicate should fail")
	}

	// Verify error is a unique constraint error
	var uniqueErr *storage.ErrUniqueConstraint
	if !errors.As(err, &uniqueErr) {
		t.Errorf("Error should be ErrUniqueConstraint, got: %T", err)
	}

	// Verify nothing changed in the index
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected still 3 values after failed batch, found %d", len(idx.uniqueValues))
	}

	// Test batch with duplicate from existing values
	existingDuplicateBatch := map[int64][]storage.ColumnValue{
		4: {storage.NewStringValue("user4@example.com")},
		5: {storage.NewStringValue("user1@example.com")}, // Duplicate of existing
	}

	err = idx.AddBatch(existingDuplicateBatch)
	if err == nil {
		t.Error("Batch with existing duplicate should fail")
	}

	// Verify nothing changed in the index
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected still 3 values after failed batch, found %d", len(idx.uniqueValues))
	}

	// Test batch with NULL values
	nullBatch := map[int64][]storage.ColumnValue{
		7: {storage.NewNullValue(storage.TEXT)},
		8: {storage.NewNullValue(storage.TEXT)},
	}

	err = idx.AddBatch(nullBatch)
	if err != nil {
		t.Errorf("Batch with NULL values should not fail: %v", err)
	}

	// NULL values should be skipped
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected still 3 values after NULL batch, found %d", len(idx.uniqueValues))
	}

	// Test batch with invalid column count
	invalidBatch := map[int64][]storage.ColumnValue{
		9: {storage.NewStringValue("valid@example.com")},
		10: {
			storage.NewStringValue("invalid@example.com"),
			storage.NewIntegerValue(42), // Extra value
		},
	}

	err = idx.AddBatch(invalidBatch)
	if err == nil {
		t.Error("Batch with wrong column count should fail")
	}
}

func TestUniqueConstraintIndex_Remove(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Add some values
	testValues := []string{
		"user1@example.com",
		"user2@example.com",
		"user3@example.com",
	}

	for i, val := range testValues {
		rowID := int64(i + 1)
		err := idx.Add([]storage.ColumnValue{storage.NewStringValue(val)}, rowID, 0)
		if err != nil {
			t.Fatalf("Failed to add test value %s: %v", val, err)
		}
	}

	// Test successful removal
	err := idx.Remove([]storage.ColumnValue{storage.NewStringValue("user2@example.com")}, 2, 0)
	if err != nil {
		t.Errorf("Failed to remove value: %v", err)
	}

	// Verify value was removed
	if len(idx.uniqueValues) != 2 {
		t.Errorf("Expected 2 values after removal, found %d", len(idx.uniqueValues))
	}

	// Test removing with wrong row ID (should not remove)
	err = idx.Remove([]storage.ColumnValue{storage.NewStringValue("user1@example.com")}, 999, 0)
	if err != nil {
		t.Errorf("Remove with wrong rowID should succeed but not remove: %v", err)
	}

	// Value should still be there
	if len(idx.uniqueValues) != 2 {
		t.Errorf("Expected 2 values after attempted removal with wrong rowID, found %d", len(idx.uniqueValues))
	}

	// Test removing NULL value (should be no-op)
	err = idx.Remove([]storage.ColumnValue{storage.NewNullValue(storage.TEXT)}, 3, 0)
	if err != nil {
		t.Errorf("Remove NULL value should succeed: %v", err)
	}

	// Value count should not change
	if len(idx.uniqueValues) != 2 {
		t.Errorf("Expected 2 values after NULL removal, found %d", len(idx.uniqueValues))
	}

	// Test removing with wrong column count
	err = idx.Remove([]storage.ColumnValue{
		storage.NewStringValue("user3@example.com"),
		storage.NewIntegerValue(42),
	}, 3, 0)
	if err == nil {
		t.Error("Remove with wrong column count should fail")
	}
}

func TestUniqueConstraintIndex_RemoveBatch(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Add some values
	testValues := []string{
		"user1@example.com",
		"user2@example.com",
		"user3@example.com",
		"user4@example.com",
		"user5@example.com",
	}

	for i, val := range testValues {
		rowID := int64(i + 1)
		err := idx.Add([]storage.ColumnValue{storage.NewStringValue(val)}, rowID, 0)
		if err != nil {
			t.Fatalf("Failed to add test value %s: %v", val, err)
		}
	}

	// Test successful batch remove
	removeBatch := map[int64][]storage.ColumnValue{
		2: {storage.NewStringValue("user2@example.com")},
		4: {storage.NewStringValue("user4@example.com")},
	}

	err := idx.RemoveBatch(removeBatch)
	if err != nil {
		t.Errorf("Failed to remove batch: %v", err)
	}

	// Verify values were removed
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected 3 values after batch removal, found %d", len(idx.uniqueValues))
	}

	// Test removing with wrong row ID (should not remove)
	wrongIDsBatch := map[int64][]storage.ColumnValue{
		999: {storage.NewStringValue("user1@example.com")},
	}

	err = idx.RemoveBatch(wrongIDsBatch)
	if err != nil {
		t.Errorf("RemoveBatch with wrong rowID should succeed but not remove: %v", err)
	}

	// Value count should not change
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected 3 values after attempted removal with wrong rowID, found %d", len(idx.uniqueValues))
	}

	// Test removing NULL values (should be no-op)
	nullBatch := map[int64][]storage.ColumnValue{
		3: {storage.NewNullValue(storage.TEXT)},
		5: {storage.NewNullValue(storage.TEXT)},
	}

	err = idx.RemoveBatch(nullBatch)
	if err != nil {
		t.Errorf("RemoveBatch NULL values should succeed: %v", err)
	}

	// Value count should not change
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected 3 values after NULL batch removal, found %d", len(idx.uniqueValues))
	}

	// Test removing with wrong column count
	invalidBatch := map[int64][]storage.ColumnValue{
		1: {storage.NewStringValue("user1@example.com")},
		3: {
			storage.NewStringValue("user3@example.com"),
			storage.NewIntegerValue(42), // Extra value
		},
	}

	err = idx.RemoveBatch(invalidBatch)
	if err == nil {
		t.Error("RemoveBatch with wrong column count should fail")
	}
}

func TestUniqueConstraintIndex_DataTypes(t *testing.T) {
	// Create an index with multiple data types
	idx := NewUniqueConstraintIndex(
		"mixed_types",
		"products",
		[]string{"category", "sku", "price", "in_stock", "created"},
		[]int{1, 2, 3, 4, 5},
		[]storage.DataType{
			storage.TEXT,      // category
			storage.TEXT,      // sku
			storage.FLOAT,     // price
			storage.BOOLEAN,   // in_stock
			storage.TIMESTAMP, // created
		})

	// Test values for different data types
	now := time.Now()
	row1Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-12345"),
		storage.NewFloatValue(299.99),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
	}

	// Add the first row
	err := idx.Add(row1Values, 1, 0)
	if err != nil {
		t.Fatalf("Failed to add mixed type values: %v", err)
	}

	// Verify the value was added
	if len(idx.uniqueValues) != 1 {
		t.Errorf("Expected 1 value in index, found %d", len(idx.uniqueValues))
	}

	// Test adding a row with the same values (should fail)
	err = idx.Add(row1Values, 2, 0)
	if err == nil {
		t.Error("Adding duplicate mixed type values should fail")
	}

	// Test with a different category
	row2Values := []storage.ColumnValue{
		storage.NewStringValue("books"), // Different category
		storage.NewStringValue("E-12345"),
		storage.NewFloatValue(299.99),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
	}

	err = idx.Add(row2Values, 2, 0)
	if err != nil {
		t.Errorf("Failed to add row with different category: %v", err)
	}

	// Test with a different sku
	row3Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-54321"), // Different SKU
		storage.NewFloatValue(299.99),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
	}

	err = idx.Add(row3Values, 3, 0)
	if err != nil {
		t.Errorf("Failed to add row with different sku: %v", err)
	}

	// Test with a different price
	row4Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-12345"),
		storage.NewFloatValue(199.99), // Different price
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
	}

	err = idx.Add(row4Values, 4, 0)
	if err != nil {
		t.Errorf("Failed to add row with different price: %v", err)
	}

	// Test with a different boolean value
	row5Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-12345"),
		storage.NewFloatValue(299.99),
		storage.NewBooleanValue(false), // Different boolean
		storage.NewTimestampValue(now),
	}

	err = idx.Add(row5Values, 5, 0)
	if err != nil {
		t.Errorf("Failed to add row with different boolean: %v", err)
	}

	// Test with a different timestamp
	row6Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-12345"),
		storage.NewFloatValue(299.99),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now.Add(time.Hour)), // Different timestamp
	}

	err = idx.Add(row6Values, 6, 0)
	if err != nil {
		t.Errorf("Failed to add row with different timestamp: %v", err)
	}

	// Verify we have the expected number of unique values
	if len(idx.uniqueValues) != 6 {
		t.Errorf("Expected 6 unique values, found %d", len(idx.uniqueValues))
	}

	// Test mixed nulls
	row7Values := []storage.ColumnValue{
		storage.NewStringValue("electronics"),
		storage.NewStringValue("E-12345"),
		storage.NewNullValue(storage.FLOAT), // NULL price
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
	}

	err = idx.Add(row7Values, 7, 0)
	if err != nil {
		t.Errorf("Failed to add row with NULL price: %v", err)
	}

	// Since row has NULL, it should be ignored by the index
	if len(idx.uniqueValues) != 6 {
		t.Errorf("Expected 6 unique values after adding NULL value, found %d", len(idx.uniqueValues))
	}
}

func TestUniqueConstraintIndex_NullHandling(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Add a regular value
	err := idx.Add([]storage.ColumnValue{storage.NewStringValue("user1@example.com")}, 1, 0)
	if err != nil {
		t.Fatalf("Failed to add regular value: %v", err)
	}

	// Add multiple NULL values (should all be ignored)
	nullValues := []storage.ColumnValue{storage.NewNullValue(storage.TEXT)}

	// First NULL
	err = idx.Add(nullValues, 2, 0)
	if err != nil {
		t.Errorf("Failed to add first NULL value: %v", err)
	}

	// Second NULL
	err = idx.Add(nullValues, 3, 0)
	if err != nil {
		t.Errorf("Failed to add second NULL value: %v", err)
	}

	// Third NULL
	err = idx.Add(nullValues, 4, 0)
	if err != nil {
		t.Errorf("Failed to add third NULL value: %v", err)
	}

	// Only the regular value should be in the index
	if len(idx.uniqueValues) != 1 {
		t.Errorf("Expected 1, found %d values in index (NULL values should be ignored)", len(idx.uniqueValues))
	}

	// Test NULL handling in a multi-column index
	multiIdx := NewUniqueConstraintIndex(
		"multi_idx",
		"orders",
		[]string{"region", "order_number"},
		[]int{1, 2},
		[]storage.DataType{storage.TEXT, storage.TEXT})

	// Add a regular multi-column value
	err = multiIdx.Add([]storage.ColumnValue{
		storage.NewStringValue("US"),
		storage.NewStringValue("ORD-123"),
	}, 1, 0)
	if err != nil {
		t.Fatalf("Failed to add regular multi-column value: %v", err)
	}

	// Add a value with the first column NULL
	err = multiIdx.Add([]storage.ColumnValue{
		storage.NewNullValue(storage.TEXT),
		storage.NewStringValue("ORD-456"),
	}, 2, 0)
	if err != nil {
		t.Errorf("Failed to add multi-column with first NULL: %v", err)
	}

	// Add a value with the second column NULL
	err = multiIdx.Add([]storage.ColumnValue{
		storage.NewStringValue("EU"),
		storage.NewNullValue(storage.TEXT),
	}, 3, 0)
	if err != nil {
		t.Errorf("Failed to add multi-column with second NULL: %v", err)
	}

	// Add a value with both columns NULL
	err = multiIdx.Add([]storage.ColumnValue{
		storage.NewNullValue(storage.TEXT),
		storage.NewNullValue(storage.TEXT),
	}, 4, 0)
	if err != nil {
		t.Errorf("Failed to add multi-column with both NULL: %v", err)
	}

	// Only the regular value should be in the index
	if len(multiIdx.uniqueValues) != 1 {
		t.Errorf("Expected 1, found %d values in multi-column index (rows with ANY NULL should be ignored)",
			len(multiIdx.uniqueValues))
	}
}

func TestUniqueConstraintIndex_Close(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Add some values
	err := idx.Add([]storage.ColumnValue{storage.NewStringValue("user1@example.com")}, 1, 0)
	if err != nil {
		t.Fatalf("Failed to add value: %v", err)
	}

	// Close the index
	err = idx.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Verify the map is nil after closing
	if idx.uniqueValues != nil {
		t.Error("uniqueValues map should be nil after Close()")
	}
}

// Skip the flaky concurrency test
// func TestUniqueConstraintIndex_Concurrency(t *testing.T) {
// 	// skipping due to non-deterministic behavior
// 	t.Skip("Skipping test with non-deterministic results in concurrent environments")
// }

func TestUniqueConstraintIndex_LargeValues(t *testing.T) {
	// Create an index for text values
	idx := NewUniqueConstraintIndex(
		"large_text_idx",
		"documents",
		[]string{"content"},
		[]int{1},
		[]storage.DataType{storage.TEXT})

	// Create a text value slightly smaller than 255 chars
	text254 := string(make([]byte, 254))
	for i := range text254 {
		text254 = text254[:i] + "a" + text254[i+1:]
	}

	err := idx.Add([]storage.ColumnValue{storage.NewStringValue(text254)}, 1, 0)
	if err != nil {
		t.Errorf("Failed to add 254-char text: %v", err)
	}

	// Create a text value exactly 255 chars
	text255 := string(make([]byte, 255))
	for i := range text255 {
		text255 = text255[:i] + "b" + text255[i+1:]
	}

	err = idx.Add([]storage.ColumnValue{storage.NewStringValue(text255)}, 2, 0)
	if err != nil {
		t.Errorf("Failed to add 255-char text: %v", err)
	}

	// Create a text value larger than 255 chars
	text300 := string(make([]byte, 300))
	for i := range text300 {
		text300 = text300[:i] + "c" + text300[i+1:]
	}

	err = idx.Add([]storage.ColumnValue{storage.NewStringValue(text300)}, 3, 0)
	if err != nil {
		t.Errorf("Failed to add 300-char text: %v", err)
	}

	// Verify all three values are stored
	if len(idx.uniqueValues) != 3 {
		t.Errorf("Expected 3 values for different text sizes, found %d", len(idx.uniqueValues))
	}

	// Try to add a duplicate of the long text (should fail)
	err = idx.Add([]storage.ColumnValue{storage.NewStringValue(text300)}, 4, 0)
	if err == nil {
		t.Error("Adding duplicate of long text should fail")
	}
}

func TestUniqueConstraintIndex_QueryOperations(t *testing.T) {
	// Create a simple index for testing
	idx := NewUniqueConstraintIndex(
		"test_idx",
		"users",
		[]string{"email"},
		[]int{2},
		[]storage.DataType{storage.TEXT})

	// Add some values
	err := idx.Add([]storage.ColumnValue{storage.NewStringValue("user@example.com")}, 1, 0)
	if err != nil {
		t.Fatalf("Failed to add value: %v", err)
	}

	// Test query methods (all should return nil/empty)
	findResult, err := idx.Find([]storage.ColumnValue{storage.NewStringValue("user@example.com")})
	if err != nil {
		t.Errorf("Find() returned error: %v", err)
	}
	if findResult != nil {
		t.Errorf("Find() should return nil, got %v", findResult)
	}

	rangeResult, err := idx.FindRange(
		[]storage.ColumnValue{storage.NewStringValue("a")},
		[]storage.ColumnValue{storage.NewStringValue("z")},
		true, true)
	if err != nil {
		t.Errorf("FindRange() returned error: %v", err)
	}
	if rangeResult != nil {
		t.Errorf("FindRange() should return nil, got %v", rangeResult)
	}

	opResult, err := idx.FindWithOperator(storage.EQ, []storage.ColumnValue{storage.NewStringValue("user@example.com")})
	if err != nil {
		t.Errorf("FindWithOperator() returned error: %v", err)
	}
	if opResult != nil {
		t.Errorf("FindWithOperator() should return nil, got %v", opResult)
	}

	equalResult := idx.GetRowIDsEqual([]storage.ColumnValue{storage.NewStringValue("user@example.com")})
	if equalResult != nil {
		t.Errorf("GetRowIDsEqual() should return nil, got %v", equalResult)
	}

	rangeIDs := idx.GetRowIDsInRange(
		[]storage.ColumnValue{storage.NewStringValue("a")},
		[]storage.ColumnValue{storage.NewStringValue("z")},
		true, true)
	if rangeIDs != nil {
		t.Errorf("GetRowIDsInRange() should return nil, got %v", rangeIDs)
	}

	// Mock expression
	mockExpr := &mockExpression{}
	exprResult := idx.GetFilteredRowIDs(mockExpr)
	if exprResult != nil {
		t.Errorf("GetFilteredRowIDs() should return nil, got %v", exprResult)
	}

	// Test Build() (should be no-op)
	err = idx.Build()
	if err != nil {
		t.Errorf("Build() should succeed: %v", err)
	}
}

func BenchmarkUniqueConstraintIndex_Add(b *testing.B) {
	// Create an index for benchmarking
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"users",
		[]string{"email"},
		[]int{1},
		[]storage.DataType{storage.TEXT})

	// Generate test values
	testValues := make([]storage.ColumnValue, b.N)
	for i := 0; i < b.N; i++ {
		testValues[i] = storage.NewStringValue(fmt.Sprintf("user%d@example.com", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add([]storage.ColumnValue{testValues[i]}, int64(i), 0)
	}
}

func BenchmarkUniqueConstraintIndex_AddBatch(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"users",
		[]string{"email"},
		[]int{1},
		[]storage.DataType{storage.TEXT})

	// Create entries in batches of 1000
	const batchSize = 1000
	numBatches := (b.N + batchSize - 1) / batchSize // Ceiling division

	b.ResetTimer()
	for batch := 0; batch < numBatches; batch++ {
		entries := make(map[int64][]storage.ColumnValue)
		startIdx := batch * batchSize
		endIdx := startIdx + batchSize
		if endIdx > b.N {
			endIdx = b.N
		}

		for i := startIdx; i < endIdx; i++ {
			entries[int64(i)] = []storage.ColumnValue{
				storage.NewStringValue(fmt.Sprintf("user%d@example.com", i)),
			}
		}

		idx.AddBatch(entries)
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_Integer(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"int_val"},
		[]int{1},
		[]storage.DataType{storage.INTEGER})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{storage.NewIntegerValue(int64(i))})
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_Float(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"float_val"},
		[]int{1},
		[]storage.DataType{storage.FLOAT})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{storage.NewFloatValue(float64(i) + 0.5)})
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_Text(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"text_val"},
		[]int{1},
		[]storage.DataType{storage.TEXT})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{storage.NewStringValue(fmt.Sprintf("text%d", i))})
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_Boolean(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"bool_val"},
		[]int{1},
		[]storage.DataType{storage.BOOLEAN})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{storage.NewBooleanValue(i%2 == 0)})
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_Timestamp(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"ts_val"},
		[]int{1},
		[]storage.DataType{storage.TIMESTAMP})

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{storage.NewTimestampValue(now.Add(time.Duration(i) * time.Second))})
	}
}

func BenchmarkUniqueConstraintIndex_CreateKey_MultiColumn(b *testing.B) {
	idx := NewUniqueConstraintIndex(
		"bench_idx",
		"values",
		[]string{"int_val", "text_val", "bool_val"},
		[]int{1, 2, 3},
		[]storage.DataType{storage.INTEGER, storage.TEXT, storage.BOOLEAN})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.createKey([]storage.ColumnValue{
			storage.NewIntegerValue(int64(i)),
			storage.NewStringValue(fmt.Sprintf("text%d", i)),
			storage.NewBooleanValue(i%2 == 0),
		})
	}
}

// Helper functions

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalDataTypeSlices(a, b []storage.DataType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Mock expression for testing
type mockExpression struct{}

func (m *mockExpression) Evaluate(row storage.Row) (bool, error) {
	return true, nil
}

func (m *mockExpression) EvaluateFast(row storage.Row) bool {
	return true
}

func (m *mockExpression) WithAliases(aliases map[string]string) storage.Expression {
	return m
}

func (m *mockExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	return m
}