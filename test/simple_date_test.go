package test

import (
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func TestSimpleDateTest(t *testing.T) {
	// Initialize in-memory database
	db, err := sql.Open("stoolap", "db://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a simple test table with just date column
	_, err = db.Exec(`
		CREATE TABLE simple_date_test (
			id INTEGER,
			date_val DATE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data using SQL
	_, err = db.Exec(`
		INSERT INTO simple_date_test (id, date_val) VALUES
		(1, '2023-01-15')
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Log("Successfully inserted test data")

	// Verify the row count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM simple_date_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	t.Logf("Table has %d rows", count)

	// Add a debug query to see all rows in the table
	debugRows, err := db.Query("SELECT id, date_val FROM simple_date_test")
	if err != nil {
		t.Fatalf("Failed to debug query all rows: %v", err)
	}

	t.Log("TEST-LOG: All rows in table:")

	var dateVal string
	for debugRows.Next() {
		var id int64
		// Scan the row into variables
		if err := debugRows.Scan(&id, &dateVal); err != nil {
			t.Fatalf("Failed to scan debug row: %v", err)
		}
		t.Logf("TEST-LOG: Row data: ID=%T, DATE=%T", id, dateVal)
	}
	debugRows.Close()

	if dateVal != "2023-01-15" {
		t.Errorf("Expected date value '2023-01-15', got '%s'", dateVal)
	}
}
