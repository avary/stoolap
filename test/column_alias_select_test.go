package test

import (
	"database/sql"
	"testing"

	_ "github.com/semihalev/stoolap/pkg/driver" // Import for database registration
)

func TestColumnAliasInSelect(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "db://var/tmp/test_column_alias.db")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE items (id INTEGER, price INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO items (id, price, name) VALUES (1, 100, 'Item A')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Test simple SELECT with column alias
	t.Log("Executing: SELECT price AS cost FROM items")
	rows, err := db.Query(`SELECT price FROM items`)
	if err != nil {
		t.Fatalf("Error selecting without alias: %v", err)
	}

	// See if a regular select works
	cols, _ := rows.Columns()
	t.Logf("Regular SELECT columns: %v", cols)
	rows.Close()

	// Now try with an alias - use a query string variable for tracing
	aliasQuery := "SELECT price AS cost FROM items"
	t.Logf("Now trying with alias: %s", aliasQuery)
	rows, err = db.Query(aliasQuery)
	if err != nil {
		t.Logf("Error details: %+v", err)
		t.Fatalf("Error executing SELECT with alias: %v", err)
	}
	defer rows.Close()

	// Debug: print column information as received
	t.Log("Debug: Successfully executed query with alias")

	// Get column names from result set
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Error getting column names: %v", err)
	}

	// Verify that the column is named "cost", not "price"
	if len(columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(columns))
	} else if columns[0] != "cost" {
		t.Errorf("Expected column name to be 'cost', got '%s'", columns[0])
	}

	// Read the row and verify the value
	var cost int
	if rows.Next() {
		err := rows.Scan(&cost)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}

		if cost != 100 {
			t.Errorf("Expected cost = 100, got %d", cost)
		}
	} else {
		t.Errorf("Expected 1 row, got none")
	}
}
