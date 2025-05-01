package test

import (
	"database/sql"
	"testing"

	_ "github.com/semihalev/stoolap/pkg/driver" // Import for database registration
)

func TestDebugColumnAliases(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "db:///:memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE debug_products (
		id INTEGER,
		price INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO debug_products (id, price, name) VALUES (1, 150, 'Expensive')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Try to get the column metadata directly
	rows, err := db.Query(`SELECT price AS cost FROM debug_products`)
	if err != nil {
		t.Fatalf("Error executing simple query: %v", err)
	}

	// Print column information
	_, err = rows.Columns()
	if err != nil {
		t.Fatalf("Error getting columns: %v", err)
	}
	rows.Close()

	// Try with WHERE clause
	rows, err = db.Query(`SELECT price AS cost FROM debug_products WHERE cost > 100`)
	if err != nil {
		t.Fatalf("Expected error with WHERE alias: %v\n", err)
	} else {
		defer rows.Close()

		// Should not get here if there's an error
		_, err := rows.Columns()
		if err != nil {
			t.Fatalf("Error getting columns: %v", err)
		}

		// Try to read the data
		var cost int
		for rows.Next() {
			err := rows.Scan(&cost)
			if err != nil {
				t.Fatalf("Error scanning row: %v", err)
			}
		}
	}
}
