---
title: PRAGMA Commands
category: SQL Commands
order: 1
---

# PRAGMA Commands

This document describes the PRAGMA commands available in Stoolap based on implementations and test cases.

## Overview

Stoolap provides PRAGMA commands for configuring and inspecting the database engine. These commands primarily focus on persistence settings and storage behavior.

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

Stoolap currently supports the following PRAGMA commands:

### Snapshot and WAL Configuration

#### snapshot_interval

Controls how often the database creates snapshots of the data (in seconds).

```sql
-- Set snapshot interval to 60 seconds
PRAGMA snapshot_interval = 60;

-- Get current snapshot interval
PRAGMA snapshot_interval;
```

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

#### keep_snapshots

Controls how many snapshots to retain for each table:

```sql
-- Keep 5 snapshots per table
PRAGMA keep_snapshots = 5;

-- Get current number of snapshots kept
PRAGMA keep_snapshots;
```

#### wal_flush_trigger

Controls the number of operations before the WAL is flushed to disk:

```sql
-- Set WAL flush trigger to 1000 operations
PRAGMA wal_flush_trigger = 1000;

-- Get current WAL flush trigger
PRAGMA wal_flush_trigger;
```

## Examples

### Basic PRAGMA Usage

```sql
-- Set snapshot interval to 60 seconds
PRAGMA snapshot_interval = 60;

-- Verify the setting
PRAGMA snapshot_interval;
```

### Multiple PRAGMA Commands

```sql
-- Set sync mode to full
PRAGMA sync_mode = 2;

-- Keep 10 snapshots per table
PRAGMA keep_snapshots = 10;

-- Set WAL flush trigger to 1000 operations
PRAGMA wal_flush_trigger = 1000;
```

## PRAGMA Persistence

PRAGMA settings are persisted for file-based and db:// connections, but reset for each new connection. If you want settings to persist across database restarts, you should execute PRAGMA commands after opening the connection.

## Best Practices

1. **Tune Snapshot Interval**: Adjust `snapshot_interval` based on your workload. Lower values provide better durability but more I/O overhead.

2. **Choose Appropriate Sync Mode**: 
   - Use `sync_mode = 2` for critical data where durability is paramount
   - Use `sync_mode = 1` for most applications (good balance)
   - Use `sync_mode = 0` only for non-critical data or testing

3. **Manage Snapshots**: Set `keep_snapshots` based on your backup needs and disk space constraints.

4. **Apply PRAGMA at Startup**: Run important PRAGMA commands right after opening the database connection.

## Implementation Details

PRAGMA commands are handled directly by the storage engine and affect the persistence behavior of the database. They do not require transactions and take effect immediately after being set.