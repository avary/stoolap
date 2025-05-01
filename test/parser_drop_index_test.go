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

// TestDropIndexIfExists tests the DROP INDEX IF EXISTS statement
func TestDropIndexIfExists(t *testing.T) {
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
	result, err := executor.Execute(context.Background(), nil, `
		CREATE TABLE test_drop_index (
			id INTEGER, 
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Try to drop a nonexistent index with IF EXISTS (should succeed)
	result, err = executor.Execute(context.Background(), nil, `
		DROP INDEX IF EXISTS nonexistent_idx ON test_drop_index
	`)
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS failed: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Try to drop a nonexistent index without IF EXISTS (should fail)
	result, err = executor.Execute(context.Background(), nil, `
		DROP INDEX nonexistent_idx ON test_drop_index
	`)
	if err == nil {
		if result != nil {
			result.Close()
		}
		t.Fatalf("Expected error when dropping nonexistent index without IF EXISTS, but got none")
	}

	t.Logf("DROP INDEX IF EXISTS test passed!")
}
