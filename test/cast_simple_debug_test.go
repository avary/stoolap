package test

import (
	"context"
	"testing"

	"github.com/semihalev/stoolap/pkg"
)

func TestCastSimpleDebug(t *testing.T) {
	// Create an in-memory database
	ctx := context.Background()
	db, _ := pkg.Open("db://var/tmp/test_cast_simple_debug.db")
	defer db.Close()

	executor := db.Executor()

	// Create a table with known values
	executor.Execute(ctx, nil, `
		CREATE TABLE test_simple (
			id INTEGER,
			text_val TEXT
		)
	`)

	// Insert a single value
	executor.Execute(ctx, nil, `
		INSERT INTO test_simple (id, text_val) 
		VALUES (1, '123')
	`)

	// Test a simple CAST using a literal
	result, _ := executor.Execute(ctx, nil, "SELECT CAST('123' AS INTEGER) FROM test_simple")
	if result.Next() {
		var val interface{}
		result.Scan(&val)
		t.Logf("CAST('123' AS INTEGER) = %v (type: %T)", val, val)
	}

	// Test a CAST on a column
	result, _ = executor.Execute(ctx, nil, "SELECT CAST(text_val AS INTEGER) FROM test_simple")
	if result.Next() {
		var val interface{}
		result.Scan(&val)
		t.Logf("CAST(text_val AS INTEGER) = %v (type: %T)", val, val)
	}

	// Test a CAST in a WHERE clause
	result, _ = executor.Execute(ctx, nil, "SELECT id FROM test_simple WHERE CAST(text_val AS INTEGER) > 100")
	if result.Next() {
		var id int64
		result.Scan(&id)
		t.Logf("Found id %d where CAST(text_val AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row where CAST(text_val AS INTEGER) > 100, but none found")
	}

	// Test a CAST with a fixed value in a WHERE clause
	result, _ = executor.Execute(ctx, nil, "SELECT id FROM test_simple WHERE CAST('123' AS INTEGER) > 100")
	if result.Next() {
		var id int64
		result.Scan(&id)
		t.Logf("Found id %d where CAST('123' AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row where CAST('123' AS INTEGER) > 100, but none found")
	}
}
