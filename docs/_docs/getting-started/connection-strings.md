---
title: Connection String Reference
category: Getting Started
order: 1
---

# Connection String Reference

This document provides information about Stoolap connection string formats and their usage.

## Connection String Basics

Stoolap connection strings follow a URL-like format:

```
scheme://[path]
```

Where:
- `scheme` specifies the storage engine type
- `path` provides location information for persistent storage (optional for memory storage)

## Supported Storage Engine Types

Stoolap supports three storage engine types:

### In-Memory Storage (memory://)

```
memory://
```

The in-memory storage engine:
- Stores all data in RAM
- Provides maximum performance
- Data is lost when the process terminates
- No persistence between sessions

Example:
```
memory://
```

### File-Based Persistent Storage (file://)

```
file:///path/to/data
```

The file-based storage engine:
- Stores data on disk
- Provides durability across process restarts
- Balances performance with persistence
- Requires a valid file path

Examples:
```
file:///data/mydb
file:///Users/username/stoolap/data
file:///C:/stoolap/data
```

### Database Engine with MVCC (db://)

```
db:///path/to/data
```

The database engine offers:
- MVCC (Multi-Version Concurrency Control)
- Transaction isolation with optimistic concurrency control
- Snapshot-based persistence
- WAL (Write-Ahead Logging) for durability

Example:
```
db:///tmp/stoolap_data
```

## Usage Examples

### Command Line Example

```bash
# Start CLI with in-memory database
stoolap -db memory://

# Start with file-based persistent storage
stoolap -db file:///data/mydb

# Start with MVCC engine
stoolap -db db:///data/mydb
```

### Go Application Examples

#### In-Memory Database

```go
package main

import (
    "database/sql"
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

    // Use the database...
}
```

#### File-Based Persistent Database

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
    // Connect to a file-based database
    db, err := sql.Open("stoolap", "file:///path/to/database")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use the database...
}
```

#### MVCC-Based Database

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
    // Connect using the db:// scheme for MVCC support
    db, err := sql.Open("stoolap", "db:///path/to/database")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use the database with transaction support...
}
```

## PRAGMA Configuration

While connection strings don't support query parameters, you can configure the database using PRAGMA commands after connection:

```go
// Open database
db, err := sql.Open("stoolap", "db:///data/mydb")
if err != nil {
    log.Fatal(err)
}

// Configure database settings
_, err = db.Exec("PRAGMA snapshot_interval = 60")
if err != nil {
    log.Fatal(err)
}

// Configure WAL synchronization mode
_, err = db.Exec("PRAGMA sync_mode = 2")
if err != nil {
    log.Fatal(err)
}
```

See the [PRAGMA Commands](pragma-commands) documentation for more details on available configuration options.