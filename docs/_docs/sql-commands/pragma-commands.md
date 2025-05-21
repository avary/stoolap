---
title: PRAGMA Commands
category: SQL Commands
order: 1
---

# PRAGMA Commands

This document describes the PRAGMA commands available in Stoolap based on test files and implementation details.

## Overview

Based on test files (`/test/pragma_test.go`), Stoolap provides PRAGMA commands for configuring and inspecting the database engine. These commands allow you to adjust various settings that affect performance, durability, and behavior of the database.

## Syntax

The basic syntax for PRAGMA commands is:

```sql
PRAGMA [pragma_name] = [value];
```

or to retrieve the current value:

```sql
PRAGMA [pragma_name];
```

## Available PRAGMA Commands

Based on the test files and implementation, Stoolap supports the following PRAGMA commands:

### Snapshot and WAL Configuration

#### snapshot_interval

Controls how often the database creates snapshots of the data (in seconds).

```sql
-- Set snapshot interval to 60 seconds
PRAGMA snapshot_interval = 60;

-- Get current snapshot interval
PRAGMA snapshot_interval;
```

Default value: 300 (5 minutes)

#### sync_mode

Controls the synchronization mode for the Write-Ahead Log (WAL):

```sql
-- Set sync mode to 1 (normal)
PRAGMA sync_mode = 1;

-- Get current sync mode
PRAGMA sync_mode;
```

Supported values:
- 0: No sync (fastest, but risks data loss on power failure)
- 1: Normal sync (balances performance and durability)
- 2: Full sync (maximum durability, slowest performance)

Default value: 1 (normal)

#### keep_snapshots

Controls how many snapshots to retain for each table:

```sql
-- Keep 5 snapshots per table
PRAGMA keep_snapshots = 5;

-- Get current number of snapshots kept
PRAGMA keep_snapshots;
```

Default value: 3

### Memory Management

#### max_memory

Controls the maximum memory usage (in MiB) for the database engine:

```sql
-- Set maximum memory to 1024 MiB (1 GiB)
PRAGMA max_memory = 1024;

-- Get current maximum memory setting
PRAGMA max_memory;
```

Default value: System dependent (typically a percentage of total RAM)

### Cache Configuration

#### query_cache_enabled

Enables or disables the query result cache:

```sql
-- Enable query cache
PRAGMA query_cache_enabled = 1;

-- Disable query cache
PRAGMA query_cache_enabled = 0;

-- Get current query cache status
PRAGMA query_cache_enabled;
```

Default value: 1 (enabled)

#### query_cache_size

Controls the maximum number of cached query results:

```sql
-- Set query cache size to 100 results
PRAGMA query_cache_size = 100;

-- Get current query cache size
PRAGMA query_cache_size;
```

Default value: 50

### Table and Index Options

#### auto_vacuum

Controls the automatic vacuuming of the database to reclaim space:

```sql
-- Enable automatic vacuuming
PRAGMA auto_vacuum = 1;

-- Disable automatic vacuuming
PRAGMA auto_vacuum = 0;

-- Get current auto_vacuum setting
PRAGMA auto_vacuum;
```

Default value: 1 (enabled)

## Examples from Test Files

These examples are taken from `/test/pragma_test.go`:

### Basic PRAGMA Usage

```go
// Set and retrieve snapshot interval
_, err = db.Exec("PRAGMA snapshot_interval = 60")
if err != nil {
    t.Fatalf("Failed to set snapshot_interval: %v", err)
}

var interval int
err = db.QueryRow("PRAGMA snapshot_interval").Scan(&interval)
if err != nil {
    t.Fatalf("Failed to get snapshot_interval: %v", err)
}
if interval != 60 {
    t.Errorf("Expected snapshot_interval = 60, got %d", interval)
}
```

### Multiple PRAGMA Commands

```go
// Set multiple PRAGMA values
_, err = db.Exec("PRAGMA sync_mode = 2")
if err != nil {
    t.Fatalf("Failed to set sync_mode: %v", err)
}

_, err = db.Exec("PRAGMA keep_snapshots = 5")
if err != nil {
    t.Fatalf("Failed to set keep_snapshots: %v", err)
}

// Verify values
var syncMode int
err = db.QueryRow("PRAGMA sync_mode").Scan(&syncMode)
if err != nil {
    t.Fatalf("Failed to get sync_mode: %v", err)
}
if syncMode != 2 {
    t.Errorf("Expected sync_mode = 2, got %d", syncMode)
}

var keepSnapshots int
err = db.QueryRow("PRAGMA keep_snapshots").Scan(&keepSnapshots)
if err != nil {
    t.Fatalf("Failed to get keep_snapshots: %v", err)
}
if keepSnapshots != 5 {
    t.Errorf("Expected keep_snapshots = 5, got %d", keepSnapshots)
}
```

## PRAGMA Persistence

PRAGMA settings are persisted for file-based and db:// connections, but reset for each new connection. If you want settings to persist across database restarts, you should execute PRAGMA commands after opening the connection.

## Best Practices

Based on the implemented tests and behaviors:

1. **Tune Snapshot Interval**: Adjust `snapshot_interval` based on your workload. Lower values provide better durability but more I/O overhead.

2. **Choose Appropriate Sync Mode**: 
   - Use `sync_mode = 2` for critical data where durability is paramount
   - Use `sync_mode = 1` for most applications (good balance)
   - Use `sync_mode = 0` only for non-critical data or testing

3. **Memory Management**: Set `max_memory` based on your system resources and workload requirements

4. **Query Cache**: Enable query cache for read-heavy workloads with repetitive queries

5. **Apply PRAGMA at Startup**: Run important PRAGMA commands right after opening the database connection

## Implementation Details

From the test files and code inspection:

- PRAGMA commands are parsed in the SQL parser
- Implementation is in the engine and executor components
- Settings affect various subsystems (MVCC, query engine, etc.)
- Some PRAGMA commands take effect immediately, others on next operation