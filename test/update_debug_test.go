package test

import (
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver" // Import the Stoolap driver
)

func TestUpdateArithmeticDebug(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`CREATE TABLE debug_test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO debug_test (id, value, name) VALUES (1, 10, 'hello')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple column reference",
			query: "UPDATE debug_test SET value = value WHERE id = 1",
		},
		{
			name:  "Simple arithmetic",
			query: "UPDATE debug_test SET value = 20 WHERE id = 1",
		},
		{
			name:  "Column plus number",
			query: "UPDATE debug_test SET value = value + 5 WHERE id = 1",
		},
		{
			name:  "More complex arithmetic",
			query: "UPDATE debug_test SET value = value * 2 + 5 WHERE id = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(tt.query)
			if err != nil {
				t.Logf("❌ Query failed: %s", tt.query)
				t.Logf("Error: %v", err)
			} else {
				t.Logf("✅ Query succeeded: %s", tt.query)

				// Check the result
				var value int
				err = db.QueryRow("SELECT value FROM debug_test WHERE id = 1").Scan(&value)
				if err != nil {
					t.Logf("Failed to query result: %v", err)
				} else {
					t.Logf("New value: %d", value)
				}
			}
		})
	}
}
