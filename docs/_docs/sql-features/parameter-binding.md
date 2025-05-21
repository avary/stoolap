---
title: Parameter Binding
category: SQL Features
order: 1
---

# Parameter Binding

This document explains parameter binding in Stoolap based on the implementation and test files.

## Overview

Based on test files (`/test/parameter_binding_test.go`, `/test/parameter_location_test.go`, `/test/parameter_binding_benchmark_test.go`), Stoolap supports parameter binding for SQL statements, allowing you to create parameterized queries that are both more secure (preventing SQL injection) and more efficient (allowing for statement reuse and caching).

## Syntax

Stoolap supports positional parameters using the question mark (`?`) syntax:

```sql
-- Basic parameter usage
SELECT * FROM users WHERE id = ?;

-- Multiple parameters
SELECT * FROM users WHERE age > ? AND department = ?;

-- Parameters in different clauses
SELECT * FROM products WHERE category = ? ORDER BY price LIMIT ?;
```

## Supported Data Types

Based on the test files, the following Go types can be bound as parameters:

| Go Type | SQL Type | Example |
|---------|----------|---------|
| `int`, `int64` | INTEGER | `42` |
| `float64` | FLOAT | `3.14159` |
| `string` | TEXT | `"hello"` |
| `bool` | BOOLEAN | `true` |
| `time.Time` | TIMESTAMP | `time.Now()` |
| `nil` | NULL | `nil` |
| `[]byte` | BLOB | `[]byte{1, 2, 3}` |
| Maps/structs | JSON | `map[string]interface{}{"name": "John"}` |

## Using Parameters in Go Code

### With database/sql Package

```go
// Open database connection
db, err := sql.Open("stoolap", "memory://")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Query with parameters
rows, err := db.Query("SELECT * FROM users WHERE age > ? AND department = ?", 30, "Engineering")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Insert with parameters
_, err = db.Exec("INSERT INTO users (name, age, department) VALUES (?, ?, ?)", 
                "Jane Smith", 32, "Marketing")
if err != nil {
    log.Fatal(err)
}

// Prepared statement with parameters
stmt, err := db.Prepare("SELECT * FROM users WHERE department = ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute prepared statement multiple times with different parameters
rows, err = stmt.Query("Engineering")
// ...
rows, err = stmt.Query("Marketing")
// ...
```

### With Transactions

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute within transaction
_, err = tx.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", 101, 1000.00)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 500.00, 101)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit transaction
if err = tx.Commit(); err != nil {
    log.Fatal(err)
}
```

## Examples from Test Files

These examples are taken directly from test files:

### Basic Parameter Binding

From `/test/parameter_binding_test.go`:

```go
// Create test table
db.Exec(`CREATE TABLE test_params (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value FLOAT,
    active BOOLEAN
)`)

// Insert with parameters
db.Exec("INSERT INTO test_params VALUES (?, ?, ?, ?)", 1, "Test", 42.5, true)

// Select with parameters
var id int
var name string
var value float64
var active bool

err = db.QueryRow("SELECT * FROM test_params WHERE id = ?", 1).
    Scan(&id, &name, &value, &active)
if err != nil {
    t.Fatalf("Error querying with parameter: %v", err)
}

// Verify values
if id != 1 || name != "Test" || value != 42.5 || !active {
    t.Errorf("Unexpected result: id=%d name=%s value=%f active=%v",
        id, name, value, active)
}
```

### NULL Value Parameters

From `/test/parameter_binding_test.go`:

```go
// Insert with NULL parameter
_, err = db.Exec("INSERT INTO test_params VALUES (?, ?, ?, ?)", 
                2, "NullTest", nil, false)
if err != nil {
    t.Fatalf("Error inserting NULL parameter: %v", err)
}

// Query row with NULL
var nullableValue sql.NullFloat64
err = db.QueryRow("SELECT value FROM test_params WHERE id = ?", 2).
    Scan(&nullableValue)
if err != nil {
    t.Fatalf("Error querying row with NULL: %v", err)
}

if nullableValue.Valid {
    t.Errorf("Expected NULL value, got %f", nullableValue.Float64)
}
```

### Parameters in Different Clauses

From `/test/parameter_location_test.go`:

```go
// Parameters in SELECT, WHERE, ORDER BY, and LIMIT clauses
query := `
    SELECT 
        id, 
        value * ? AS scaled_value 
    FROM test_locations 
    WHERE 
        category = ? AND 
        value BETWEEN ? AND ?
    ORDER BY value * ? DESC
    LIMIT ?
`
// Execute with parameters in different locations
rows, err := db.Query(query, 2.0, "A", 10.0, 50.0, 2.0, 3)
if err != nil {
    t.Fatalf("Error executing query with parameters in different clauses: %v", err)
}
```

## Performance Considerations

Based on `/test/parameter_binding_benchmark_test.go`:

1. **Query Caching**: Stoolap caches the parsed form of parameterized queries, making repeated execution with different parameter values more efficient.

2. **Value Pooling**: Parameter values use a value pool to reduce memory allocations.

3. **Prepared Statements**: For best performance with repeated queries, use prepared statements.

```go
// Prepare once, execute many times
stmt, err := db.Prepare("SELECT * FROM large_table WHERE id > ? LIMIT ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute with different parameters
for i := 0; i < 100; i++ {
    rows, err := stmt.Query(i*1000, 50)
    // Process rows
    rows.Close()
}
```

## Best Practices

1. **Always Use Parameters for User Input**: To prevent SQL injection, never concatenate user-supplied values directly into SQL strings.

2. **Match Parameter Count**: Ensure the number of `?` placeholders matches the number of parameter values provided.

3. **Parameter Type Matching**: Provide parameters of appropriate types that match your column definitions.

4. **Use Prepared Statements**: For queries that will be executed multiple times with different parameters, use prepared statements.

5. **Consider Transaction Context**: For multiple related operations, use a transaction to ensure atomicity.

6. **NULL Parameter Handling**: Use `nil` to represent NULL values in parameters. When reading potentially NULL values, use the appropriate `sql.NullXXX` types.

## Limitations

1. **Named Parameters**: Currently, only positional parameters (`?`) are supported. Named parameters (`:name` or `@name` style) are not implemented.

2. **Parameter Location Awareness**: Parameters are tracked by position in the SQL statement, regardless of the order in which they appear in different clauses.

## Implementation Details

- Parameters are processed during SQL parsing and represented in the abstract syntax tree.
- During execution, parameter values are bound to their corresponding placeholders.
- Prepared statements cache the parsed SQL for efficient reuse with different parameter values.
- Parameter pooling reduces memory allocations for frequently executed statements.