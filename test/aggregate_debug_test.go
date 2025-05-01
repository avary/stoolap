package test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/semihalev/stoolap/internal/functions/registry"
	_ "github.com/semihalev/stoolap/pkg/driver" // Register database driver
)

// TestAggregationDebug is a debug test to help diagnose issues with aggregation functions
func TestAggregationDebug(t *testing.T) {
	// Create a test database
	db, err := sql.Open("stoolap", "db://var/tmp/test_agg.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a simple table
	_, err = db.Exec(`CREATE TABLE test_agg (
		id INTEGER,
		num INTEGER,
		val FLOAT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some test data
	data := []struct {
		id  int
		num int
		val float64
	}{
		{1, 10, 1.5},
		{2, 20, 2.5},
		{3, 30, 3.5},
	}

	for _, d := range data {
		_, err = db.Exec("INSERT INTO test_agg (id, num, val) VALUES (?, ?, ?)",
			d.id, d.num, d.val)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Verify the data was inserted
	rows, err := db.Query("SELECT * FROM test_agg")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, num int
		var val float64
		if err := rows.Scan(&id, &num, &val); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Data: id=%d, num=%d, val=%f", id, num, val)
		count++
	}
	t.Logf("Total rows: %d", count)

	// Debug registry
	funcRegistry := registry.GetGlobal()
	t.Logf("Function registry: %v", funcRegistry)

	// Print available aggregate functions
	for _, name := range []string{"SUM", "AVG", "COUNT", "MIN", "MAX"} {
		fn := funcRegistry.GetAggregateFunction(name)
		t.Logf("Aggregate function %s: %v", name, fn)
	}

	// Test data preparation done, now test SUM and other aggregate functions
	testCases := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{"Sum with INTEGER", "SELECT SUM(num) FROM test_agg", int64(60)},
		{"Sum with FLOAT", "SELECT SUM(val) FROM test_agg", float64(7.5)},
		{"Count all", "SELECT COUNT(*) FROM test_agg", int64(3)},
		{"Count col", "SELECT COUNT(num) FROM test_agg", int64(3)},
		{"Average", "SELECT AVG(num) FROM test_agg", float64(20)},
		{"Min", "SELECT MIN(num) FROM test_agg", int64(10)},
		{"Max", "SELECT MAX(num) FROM test_agg", int64(30)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the test query
			rows, err := db.Query(tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query '%s': %v", tc.query, err)
			}
			defer rows.Close()

			// Get column names for debugging
			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("Failed to get columns: %v", err)
			}
			t.Logf("Columns: %s", strings.Join(cols, ", "))

			// Fetch the result
			if !rows.Next() {
				t.Fatalf("No rows returned for query: %s", tc.query)
			}

			// Scan the result
			var result interface{}
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}

			// Debug output
			t.Logf("Result for %s: %v (type: %T)", tc.name, result, result)

			// Check if we have the expected type
			switch expected := tc.expected.(type) {
			case int64:
				// Check if result is an integer type
				if actual, ok := result.(int64); ok {
					if actual != expected {
						t.Errorf("Expected %d, got %d", expected, actual)
					}
				} else {
					t.Errorf("Expected int64, got %T: %v", result, result)
				}
			case float64:
				// Check if result is a float type
				if actual, ok := result.(float64); ok {
					if actual != expected {
						t.Errorf("Expected %f, got %f", expected, actual)
					}
				} else {
					t.Errorf("Expected float64, got %T: %v", result, result)
				}
			default:
				t.Errorf("Unsupported expected type: %T", expected)
			}
		})
	}

	// Print debug information about SQL execution
	queryWithDebug := "SELECT SUM(val) FROM test_agg"

	stmt, err := db.Prepare(queryWithDebug)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	rows, err = stmt.Query()
	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}
	defer rows.Close()

	// Check the result
	if rows.Next() {
		var result interface{}
		if err := rows.Scan(&result); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}
		t.Logf("SUM(val) result: %v (type: %T)", result, result)
	} else {
		t.Fatal("No result returned")
	}
}
