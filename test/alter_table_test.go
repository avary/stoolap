package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/semihalev/stoolap/internal/sql"
	"github.com/semihalev/stoolap/internal/storage"

	// Import necessary packages to register factory functions
	_ "github.com/semihalev/stoolap/internal/storage/mvcc"
)

// TestAlterTableOperations tests ALTER TABLE operations
func TestAlterTableOperations(t *testing.T) {
	t.Skip("The alter table currently not implemented")
	// Create a temporary directory for the test database
	tempDir, err := os.MkdirTemp("", "stoolap_test_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	// Get the block storage engine factory
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatalf("Failed to get db engine factory")
	}

	// Create the engine with the connection string
	engine, err := factory.Create("db://" + dbPath)
	if err != nil {
		t.Fatalf("Failed to create db engine: %v", err)
	}

	// Open the engine
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a SQL executor
	executor := sql.NewExecutor(engine)

	// Create a test table
	result, err := executor.Execute(context.Background(), nil, `CREATE TABLE test_alter (id INTEGER, name TEXT, value FLOAT)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Test adding a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter ADD COLUMN description TEXT`)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was added correctly
	schema, err := engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if the description column exists
	descriptionFound := false
	for _, col := range schema.Columns {
		if col.Name == "description" && col.Type == storage.TEXT {
			descriptionFound = true
			break
		}
	}

	if !descriptionFound {
		t.Fatalf("Added column 'description' not found in schema")
	}

	// Test dropping a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter DROP COLUMN description`)
	if err != nil {
		t.Fatalf("Failed to drop column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was dropped correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Make sure description column is no longer present
	for _, col := range schema.Columns {
		if col.Name == "description" {
			t.Fatalf("Dropped column 'description' still found in schema")
		}
	}

	// Test renaming a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter RENAME COLUMN name TO full_name`)
	if err != nil {
		t.Fatalf("Failed to rename column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was renamed correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if the full_name column exists and name doesn't
	fullNameFound := false
	for _, col := range schema.Columns {
		if col.Name == "name" {
			t.Fatalf("Old column name 'name' still found in schema")
		}
		if col.Name == "full_name" {
			fullNameFound = true
		}
	}

	if !fullNameFound {
		t.Fatalf("Renamed column 'full_name' not found in schema")
	}

	// Test modifying a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter MODIFY COLUMN value INTEGER`)
	if err != nil {
		t.Fatalf("Failed to modify column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was modified correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if value column now has INTEGER type
	valueFound := false
	for _, col := range schema.Columns {
		if col.Name == "value" {
			if col.Type != storage.INTEGER {
				t.Fatalf("Modified column 'value' has incorrect type: got %v, want %v", col.Type, storage.INTEGER)
			}
			valueFound = true
			break
		}
	}

	if !valueFound {
		t.Fatalf("Column 'value' not found in schema")
	}

	// Test renaming a table
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter RENAME TO test_renamed`)
	if err != nil {
		t.Fatalf("Failed to rename table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify table was renamed correctly by checking if the tables exist directly
	t.Log("Checking if the table was renamed correctly...")

	// Check if old table exists
	oldTableExists, err := engine.TableExists("test_alter")
	if err != nil {
		t.Fatalf("Error checking if old table exists: %v", err)
	}
	if oldTableExists {
		t.Fatalf("Old table name 'test_alter' still exists")
	}

	// Check if new table exists
	newTableExists, err := engine.TableExists("test_renamed")
	if err != nil {
		t.Fatalf("Error checking if new table exists: %v", err)
	}
	if !newTableExists {
		t.Fatalf("Renamed table 'test_renamed' not found")
	}

	t.Log("Table renaming verified successfully")
}
