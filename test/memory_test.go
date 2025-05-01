package test

import (
	"database/sql"
	"testing"

	_ "github.com/semihalev/stoolap/pkg/driver"
)

func TestMemoryDatabase(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "db://test")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test ping
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// TODO: Add more tests as functionality is implemented
}
