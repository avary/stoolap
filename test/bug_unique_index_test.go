package test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Import stoolap driver
	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestUniqueIndexBug verifies a bug where unique constraints aren't enforced
// after database restart until rows are loaded into memory via SELECT.
func TestUniqueIndexBug(t *testing.T) {
	t.Parallel()
	// Create a temporary database path
	tempDir, err := os.MkdirTemp("", "stoolap_test_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	// Use a short snapshot interval to force snapshot creation
	connString := fmt.Sprintf("file://%s?snapshot_interval=2", dbPath)
	t.Logf("Using connection string: %s", connString)

	// First database connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a test table
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		username TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a unique index on email
	t.Log("Creating unique index on email")
	_, err = db.Exec(`CREATE UNIQUE INDEX idx_users_email ON users(email)`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(1, 'user1', 'user1@example.com'),
		(2, 'user2', 'user2@example.com'),
		(3, 'user3', 'user3@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Verify uniqueness constraint works
	t.Log("Verifying uniqueness constraint works before restart")
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(4, 'duplicate', 'user1@example.com')`)
	if err == nil {
		t.Fatalf("Expected unique constraint violation, but insert succeeded")
	}
	t.Logf("Got expected error on duplicate insert before restart: %v", err)

	// Force a transaction to ensure all data is committed
	_, err = db.Exec("BEGIN TRANSACTION; COMMIT;")
	if err != nil {
		t.Logf("Note: Transaction commit returned: %v", err)
	}

	// Wait for snapshot to be created
	t.Log("Waiting 3 seconds for snapshot to be created...")
	time.Sleep(3 * time.Second)

	// Close the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	t.Log("Reopening the database")

	// Inspect the database files
	t.Log("Checking database files before reopen:")
	findPath := filepath.Join(dbPath, "users")
	files, err := os.ReadDir(findPath)
	if err != nil {
		t.Logf("Note: Could not read table directory: %v", err)
	} else {
		t.Logf("Found %d files in table directory", len(files))
		for _, file := range files {
			t.Logf("  %s", file.Name())
		}
	}

	// Check WAL directory
	walDir := filepath.Join(dbPath, "wal")
	walFiles, err := os.ReadDir(walDir)
	if err != nil {
		t.Logf("Note: Could not read WAL directory: %v", err)
	} else {
		t.Logf("Found %d files in WAL directory", len(walFiles))
		for _, file := range walFiles {
			t.Logf("  %s", file.Name())
		}
	}

	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Check what indexes the database thinks it has
	indexRows, err := db.Query(`SHOW INDEXES FROM users`)
	if err != nil {
		t.Logf("Note: Failed to query indexes: %v", err)
	} else {
		t.Log("Indexes after reopening database:")
		var count int
		for indexRows.Next() {
			count++
			var table, indexName, column, indexType string
			var isUnique bool
			err = indexRows.Scan(&table, &indexName, &column, &indexType, &isUnique)
			if err != nil {
				t.Logf("Failed to scan index row: %v", err)
				continue
			}
			t.Logf("  Index: name=%s, column=%s, type=%s, unique=%v",
				indexName, column, indexType, isUnique)
		}
		t.Logf("Found %d indexes after reopening", count)
		indexRows.Close()
	}

	// Without accessing any rows, try to insert a duplicate email
	// This should fail, but will succeed due to the bug
	t.Log("EXPECTED BUG: Trying to insert duplicate email without SELECT (should fail but will succeed)")
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(4, 'duplicate', 'user1@example.com')`)
	if err == nil {
		t.Logf("BUG CONFIRMED: Unique constraint not enforced until rows are loaded")
	} else {
		t.Logf("UNEXPECTED: Unique constraint was enforced: %v", err)
	}

	// Now run a SELECT to load the data into memory
	t.Log("Running SELECT to load rows into memory")
	rows, err := db.Query(`SELECT id, username, email FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	// Count rows and look for duplicates
	var count int
	emailSet := make(map[string]bool)
	var duplicateFound bool

	for rows.Next() {
		count++
		var id int
		var username, email string
		if err := rows.Scan(&id, &username, &email); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		t.Logf("Row: id=%d, username=%s, email=%s", id, username, email)

		// Check for duplicate emails
		if emailSet[email] {
			duplicateFound = true
			t.Logf("DUPLICATE FOUND: email=%s", email)
		}
		emailSet[email] = true
	}
	rows.Close()

	t.Logf("Found %d rows total, %d unique emails", count, len(emailSet))
	if duplicateFound {
		t.Logf("BUG CONFIRMED: Duplicate emails found in database")
	}

	// Now try to insert another duplicate, after data is loaded
	t.Log("Trying to insert another duplicate after rows are loaded")
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(5, 'another_duplicate', 'user1@example.com')`)

	if err == nil {
		t.Fatalf("Expected unique constraint violation, but second duplicate insert succeeded")
	}
	t.Logf("Constraint now working: Got expected error on second duplicate insert: %v", err)

	// Verification complete
	t.Log("Bug verification complete")
	db.Close()
}
