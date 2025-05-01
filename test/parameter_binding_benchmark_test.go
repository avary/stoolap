package test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/semihalev/stoolap/pkg"
)

func BenchmarkParameterBinding(b *testing.B) {
	// Create a memory engine for testing
	db, err := pkg.Open("db://")
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer db.Close()

	// Use the Executor method from DB
	exec := db.Executor()

	// Create a test table with different data types
	createTbl := "CREATE TABLE test_params (id INTEGER, name TEXT, salary FLOAT, active BOOLEAN, hire_date DATE, meta JSON)"
	result, err := exec.Execute(context.Background(), nil, createTbl)
	if err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}
	result.Close()

	// Parameters for typical insert
	typicalParams := []driver.NamedValue{
		{Ordinal: 1, Value: 101},
		{Ordinal: 2, Value: "Jane Smith"},
		{Ordinal: 3, Value: 85000.75},
		{Ordinal: 4, Value: false},
		{Ordinal: 5, Value: time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
		{Ordinal: 6, Value: map[string]interface{}{"department": "Engineering", "level": 3}},
	}

	// Prepare the query with parameters for all columns
	query := "INSERT INTO test_params (id, name, salary, active, hire_date, meta) VALUES (?, ?, ?, ?, ?, ?)"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := exec.ExecuteWithParams(context.Background(), nil, query, typicalParams)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
		result.Close()
	}
}
