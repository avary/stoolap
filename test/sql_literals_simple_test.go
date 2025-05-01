package test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/semihalev/stoolap/pkg/driver" // Import for database registration
)

func TestSQLLiteralsSimple(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "db:///:memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a simple test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER, value INTEGER)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test (id, value) VALUES (1, 100)`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Basic tests
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple literal",
			query: "SELECT 1",
		},
		{
			name:  "Simple column",
			query: "SELECT id FROM test",
		},
		{
			name:  "Simple function",
			query: "SELECT UPPER('hello')",
		},
		{
			name:  "Simple alias",
			query: "SELECT id AS identifier FROM test",
		},
		{
			name:  "Simple calculation",
			query: "SELECT 1 + 2",
		},
		{
			name:  "NOW function",
			query: "SELECT NOW()",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%02d_%s", i+1, test.name), func(t *testing.T) {
			t.Logf("Query: %s", test.query)

			rows, err := db.Query(test.query)
			if err != nil {
				t.Fatalf("Error executing query: %v", err)
			}
			defer rows.Close()

			columns, err := rows.Columns()
			if err != nil {
				t.Fatalf("Error getting columns: %v", err)
			}
			t.Logf("Columns: %v", columns)

			if !rows.Next() {
				t.Fatal("Expected a row but got none")
			}

			// Dynamically create scanners for the values
			values := make([]interface{}, len(columns))
			scanargs := make([]interface{}, len(columns))
			for i := range values {
				scanargs[i] = &values[i]
			}

			if err := rows.Scan(scanargs...); err != nil {
				t.Fatalf("Error scanning row: %v", err)
			}

			// Print the values
			for i, v := range values {
				t.Logf("Column %d (%s): %v (type: %T)", i, columns[i], v, v)
			}
		})
	}
}
