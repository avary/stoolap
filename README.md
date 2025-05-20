<div align="center">
  <img src="logo.svg" alt="Stoolap Logo" width="360">
</div>

<p align="center">
  <a href="https://goreportcard.com/report/github.com/stoolap/stoolap"><img src="https://goreportcard.com/badge/github.com/stoolap/stoolap?style=flat-square" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/stoolap/stoolap"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?style=flat-square" alt="go.dev reference"></a>
  <a href="https://github.com/stoolap/stoolap/releases"><img src="https://img.shields.io/github/v/release/stoolap/stoolap?style=flat-square" alt="GitHub release"></a>
  <a href="https://github.com/stoolap/stoolap/blob/main/LICENSE"><img src="https://img.shields.io/github/license/stoolap/stoolap?style=flat-square" alt="License"></a>
</p>

# Stoolap

Stoolap is a high-performance, columnar SQL database written in pure Go with zero dependencies. It combines OLTP (transaction) and OLAP (analytical) capabilities in a single engine, making it suitable for hybrid transactional/analytical processing (HTAP) workloads.

## Key Features

- **Pure Go Implementation**: Zero external dependencies for maximum portability
- **ACID Transactions**: Full transaction support with MVCC (Multi-Version Concurrency Control)
- **Fast Analytical Processing**: Columnar storage format optimized for analytical queries
- **Columnar Indexing**: Efficient single and multi-column indexes for high-performance data access
- **Memory-First Design**: Optimized for in-memory performance with optional persistence
- **Vectorized Execution**: SIMD-accelerated operations for high throughput
- **SQL Support**: Rich SQL functionality including JOINs, aggregations, and more
- **JSON Support**: Native JSON data type with optimized storage
- **Go SQL Driver**: Standard database/sql compatible driver

## Installation

```bash
go get github.com/stoolap/stoolap
```

Or clone the repository and build from source:

```bash
git clone https://github.com/stoolap/stoolap.git
cd stoolap
go build -o stoolap ./cmd/stoolap
```

## Usage

### Command Line Interface

Stoolap comes with a built-in CLI for interactive SQL queries:

```bash
# Start CLI with in-memory database
./stoolap -db memory://

# Start CLI with persistent storage
./stoolap -db file:///path/to/data
```

The CLI provides a familiar SQL interface with command history, tab completion, and formatted output.

### Go Application Integration

Stoolap can be used in Go applications using the standard database/sql interface:

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	_, err = db.Exec(`
		INSERT INTO users (id, name, email, created_at)
		VALUES (1, 'John Doe', 'john@example.com', NOW())
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Query data
	rows, err := db.Query("SELECT id, name, email FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User: %d, %s, %s\n", id, name, email)
	}
}
```

### Connection Strings

Stoolap supports three storage modes:

- **In-Memory**: `memory://` - Fast, non-persistent storage for maximum performance
- **File-Based**: `file:///path/to/data` - Durable storage with persistence
- **Database Engine**: `db:///path/to/data` - MVCC storage with transaction isolation and durability

## Supported SQL Features

### Data Types

- `INTEGER`: 64-bit signed integers
- `FLOAT`: 64-bit floating point numbers
- `TEXT`: UTF-8 encoded strings
- `BOOLEAN`: TRUE/FALSE values
- `TIMESTAMP`: Date and time values
- `JSON`: JSON-formatted data

### SQL Commands

- **DDL**: CREATE/ALTER/DROP TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW
- **DML**: SELECT, INSERT, UPDATE, DELETE, MERGE
- **Queries**: JOINs, GROUP BY, ORDER BY, LIMIT, OFFSET, subqueries, CTE (WITH), DISTINCT
- **Indexing**: CREATE INDEX, unique constraints, primary keys, multi-column indexes
- **Functions**: Aggregate and scalar functions

## Performance Optimizations

Stoolap includes numerous performance optimizations:

- **Columnar Storage**: Optimized for analytical queries and compression
- **SIMD Operations**: Vectorized execution for arithmetic, comparisons, and functions
- **Specialized Data Structures**: Custom B-trees and hashmaps for high-throughput operations
- **Expression Pushdown**: Filters pushed to storage layer for faster execution
- **Type-Specific Optimization**: Optimized operations for different data types

## Development Status

Stoolap is under active development. While it provides ACID compliance and a rich feature set, it should be considered experimental for production use.

## Documentation

Additional documentation:

- [Wiki Pages](https://github.com/stoolap/stoolap/wiki): Detailed documentation on architecture, design decisions, and usage examples

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.