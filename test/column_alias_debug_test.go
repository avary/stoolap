package test

import (
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func TestColumnAliasDebug(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables
	_, err = db.Exec(`CREATE TABLE test_items (id INTEGER, real_price INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_items (id, real_price, name) VALUES (1, 100, 'Test Item')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test 1: Basic query without alias
	t.Log("Test 1: Basic query without alias")
	rows, err := db.Query(`SELECT id, real_price FROM test_items`)
	if err != nil {
		t.Fatalf("Failed basic query: %v", err)
	}

	// Print column names
	columns, _ := rows.Columns()
	t.Logf("Column names: %v", columns)

	// Read the row
	if rows.Next() {
		var id, price int
		err = rows.Scan(&id, &price)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Row values: id=%d, price=%d", id, price)
	}
	rows.Close()

	// Test 2: Try with a different approach - aliased column with explicit column list
	t.Log("Test 2: Try with a different approach")
	_, err = db.Exec(`CREATE VIEW test_view AS SELECT id, real_price AS price FROM test_items`)
	if err != nil {
		t.Logf("NOTICE: View creation failed: %v (Views might not be supported)", err)
		// Continue with test even if view creation failed
	} else {
		rows, err = db.Query(`SELECT * FROM test_view`)
		if err != nil {
			t.Logf("View query failed: %v", err)
		} else {
			// Print column names
			columns, _ = rows.Columns()
			t.Logf("View column names: %v", columns)
			rows.Close()
		}
	}

	// Test 3: Try another workaround - direct alias
	testQuery := "SELECT id AS number, real_price AS price FROM test_items"
	rows, err = db.Query(testQuery)

	if err == nil {
		// Print column names
		columns, _ = rows.Columns()
		t.Logf("Direct alias column names: %v", columns)

		// Try to read the columns with interface{} to avoid type issues
		if rows.Next() {
			var id, price interface{}
			err = rows.Scan(&id, &price)
			if err != nil {
				t.Logf("Failed to scan row with alias: %v", err)
			} else {
				t.Logf("Success! Row values with alias: id=%v, price=%v", id, price)
			}
		}
		rows.Close()
	} else {
		t.Logf("Direct alias query failed: %v", err)
	}

	// Test 4: Try a workaround with subquery
	t.Log("Test 4: Subquery approach")
	rows, err = db.Query(`SELECT t.id, t.price FROM (SELECT id, real_price AS price FROM test_items) AS t`)
	if err == nil {
		// Print column names
		columns, _ = rows.Columns()
		t.Logf("Subquery approach column names: %v", columns)
		rows.Close()
	} else {
		t.Logf("Subquery approach failed: %v", err)
	}

	// Summary
	t.Log("Summary of findings:")
	t.Log("Column aliases are being parsed, but there's an issue with how they're stored and accessed")
	t.Log("The core issue seems to be in how storage column names are mapped to result column names")
}
