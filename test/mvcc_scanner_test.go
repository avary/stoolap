package test

import (
	"testing"

	"github.com/semihalev/stoolap/internal/storage"
	"github.com/semihalev/stoolap/internal/storage/expression"
	"github.com/semihalev/stoolap/internal/storage/mvcc"
)

func TestMVCCScannerSorting(t *testing.T) {
	// Create a schema for testing
	schema := storage.Schema{
		TableName: "test_sorting",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString, Nullable: false},
		},
	}

	// Create a map of rows with random order
	rows := map[int64]storage.Row{
		3: {
			storage.NewIntegerValue(3),
			storage.NewStringValue("Charlie"),
		},
		1: {
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
		},
		2: {
			storage.NewIntegerValue(2),
			storage.NewStringValue("Bob"),
		},
	}

	// Create a scanner
	scanner := mvcc.NewMVCCScanner(rows, schema, nil, nil)

	// Check that rows are returned in order by ID
	expectedIDs := []int64{1, 2, 3}
	expectedNames := []string{"Alice", "Bob", "Charlie"}

	rowIndex := 0
	for scanner.Next() {
		row := scanner.Row()

		// Verify ID order
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d", rowIndex)
		}
		if id != expectedIDs[rowIndex] {
			t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[rowIndex], rowIndex, id)
		}

		// Verify corresponding name
		name, ok := row[1].AsString()
		if !ok {
			t.Errorf("Failed to get name for row %d", rowIndex)
		}
		if name != expectedNames[rowIndex] {
			t.Errorf("Expected name %s at position %d, got %s", expectedNames[rowIndex], rowIndex, name)
		}

		rowIndex++
	}

	if rowIndex != 3 {
		t.Errorf("Expected 3 rows, got %d", rowIndex)
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}
}

func TestMVCCScannerProjection(t *testing.T) {
	// Create a schema for testing
	schema := storage.Schema{
		TableName: "test_projection",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString, Nullable: false},
			{ID: 2, Name: "age", Type: storage.TypeInteger, Nullable: true},
			{ID: 3, Name: "active", Type: storage.TypeBoolean, Nullable: false},
		},
	}

	// Create a map of rows
	rows := map[int64]storage.Row{
		1: {
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
			storage.NewIntegerValue(30),
			storage.NewBooleanValue(true),
		},
		2: {
			storage.NewIntegerValue(2),
			storage.NewStringValue("Bob"),
			storage.NewNullIntegerValue(),
			storage.NewBooleanValue(false),
		},
	}

	// Create a scanner with column projection (just id and name)
	scanner := mvcc.NewMVCCScanner(rows, schema, []int{0, 1}, nil)

	rowIndex := 0
	for scanner.Next() {
		row := scanner.Row()

		// Check row has only 2 columns
		if len(row) != 2 {
			t.Errorf("Expected 2 columns for projected row, got %d", len(row))
		}

		// Verify ID
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d", rowIndex)
		}
		if id != int64(rowIndex+1) {
			t.Errorf("Expected ID %d, got %d", rowIndex+1, id)
		}

		rowIndex++
	}

	if rowIndex != 2 {
		t.Errorf("Expected 2 rows, got %d", rowIndex)
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}
}

func TestMVCCScannerFiltering(t *testing.T) {
	// Create a schema for testing
	schema := storage.Schema{
		TableName: "test_filtering",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString, Nullable: false},
			{ID: 2, Name: "age", Type: storage.TypeInteger, Nullable: true},
			{ID: 3, Name: "active", Type: storage.TypeBoolean, Nullable: false},
		},
	}

	// Create a map of rows
	rows := map[int64]storage.Row{
		1: {
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
			storage.NewIntegerValue(30),
			storage.NewBooleanValue(true),
		},
		2: {
			storage.NewIntegerValue(2),
			storage.NewStringValue("Bob"),
			storage.NewNullIntegerValue(),
			storage.NewBooleanValue(false),
		},
		3: {
			storage.NewIntegerValue(3),
			storage.NewStringValue("Charlie"),
			storage.NewIntegerValue(25),
			storage.NewBooleanValue(true),
		},
	}

	// Create a filter expression for active=true
	filter := expression.NewSimpleExpression(func(row storage.Row) (bool, error) {
		active, ok := row[3].AsBoolean()
		return ok && active, nil
	})

	// Create a scanner with filtering
	scanner := mvcc.NewMVCCScanner(rows, schema, nil, filter)

	expectedIDs := []int64{1, 3}
	rowIndex := 0

	for scanner.Next() {
		row := scanner.Row()

		// Verify ID
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d", rowIndex)
		}
		if id != expectedIDs[rowIndex] {
			t.Errorf("Expected ID %d, got %d", expectedIDs[rowIndex], id)
		}

		// Verify active is true
		active, ok := row[3].AsBoolean()
		if !ok {
			t.Errorf("Failed to get active flag for row %d", rowIndex)
		}
		if !active {
			t.Errorf("Expected active=true for filtered row %d", rowIndex)
		}

		rowIndex++
	}

	if rowIndex != 2 {
		t.Errorf("Expected 2 filtered rows, got %d", rowIndex)
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}
}
