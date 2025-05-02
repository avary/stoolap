package test

import (
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func TestDebugGroupBy(t *testing.T) {
	// Connect to the database
	db, err := sql.Open("stoolap", "db:///memory")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE simple_sales (
			id INTEGER,
			amount FLOAT,
			sale_time TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data with different times
	_, err = db.Exec(`
		INSERT INTO simple_sales (id, amount, sale_time) VALUES 
		(1, 50.0, '2023-01-01 09:15:00'),
		(2, 25.0, '2023-01-01 09:30:00'),
		(3, 75.0, '2023-01-01 10:15:00')
	`)
	if err != nil {
		t.Fatalf("Failed to insert sales data: %v", err)
	}

	// Test a simple GROUP BY first
	t.Run("Test simple GROUP BY", func(t *testing.T) {
		// Group by a simple hour extraction
		rows, err := db.Query(`
			SELECT 
				EXTRACT(HOUR FROM sale_time) AS hour, 
				SUM(amount) AS total
			FROM simple_sales
			GROUP BY EXTRACT(HOUR FROM sale_time)
			ORDER BY hour
		`)
		if err != nil {
			t.Logf("Simple GROUP BY error: %v", err)
		} else {
			defer rows.Close()
			t.Log("Simple GROUP BY succeeded!")

			// Check the results
			var hour, total float64
			for rows.Next() {
				if err := rows.Scan(&hour, &total); err != nil {
					t.Logf("Error scanning row: %v", err)
				} else {
					t.Logf("Hour: %v, Total: %v", hour, total)
				}
			}
		}
	})

	// Test TIME_TRUNC in SELECT only
	t.Run("Test TIME_TRUNC in SELECT only", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT TIME_TRUNC('1h', sale_time) AS hour_bucket
			FROM simple_sales
			ORDER BY hour_bucket
		`)
		if err != nil {
			t.Fatalf("Simple TIME_TRUNC failed: %v", err)
		}
		defer rows.Close()

		// Log the results
		for rows.Next() {
			var bucket string
			if err := rows.Scan(&bucket); err != nil {
				t.Fatalf("Error scanning: %v", err)
			}
			t.Logf("Hour bucket: %s", bucket)
		}
	})

	// Test TIME_TRUNC with GROUP BY
	t.Run("Test TIME_TRUNC with GROUP BY", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT 
				TIME_TRUNC('1h', sale_time) AS hour_bucket,
				SUM(amount) AS total_sales
			FROM simple_sales
			GROUP BY TIME_TRUNC('1h', sale_time)
			ORDER BY hour_bucket
		`)
		if err != nil {
			t.Fatalf("TIME_TRUNC with GROUP BY failed: %v", err)
		}
		defer rows.Close()

		// Log the results
		for rows.Next() {
			var bucket string
			var total float64
			if err := rows.Scan(&bucket, &total); err != nil {
				t.Fatalf("Error scanning GROUP BY result: %v", err)
			}
			t.Logf("Hour bucket: %s, Total: %.2f", bucket, total)
		}
		// Log the results
		for rows.Next() {
			var bucket string
			if err := rows.Scan(&bucket); err != nil {
				t.Fatalf("Error scanning: %v", err)
			}
			t.Logf("Hour bucket: %s", bucket)
		}
	})
}
