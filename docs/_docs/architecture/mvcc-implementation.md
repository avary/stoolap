---
title: MVCC Implementation
category: Architecture
order: 1
---

# MVCC Implementation

This document provides a detailed explanation of Stoolap's Multi-Version Concurrency Control (MVCC) implementation, which enables transaction isolation with minimal locking.

## MVCC Overview

Multi-Version Concurrency Control (MVCC) is a concurrency control method used by Stoolap to provide transaction isolation. The key principles are:

1. Create a new version of data items for each update
2. Maintain multiple versions of data concurrently
3. Each transaction has a consistent view based on visibility rules
4. Reads never block writes, and writes never block reads
5. Implement optimistic concurrency control for conflict detection

## Core Components

Stoolap's MVCC implementation consists of these key components:

### Transaction Manager

- Manages transaction lifecycle (begin, commit, rollback)
- Assigns unique transaction IDs
- Tracks transaction state (active, committed, aborted)
- Implements isolation levels
- Resolves conflicts

### Version Store

- Maintains multiple versions of each row
- Associates versions with transaction IDs
- Tracks creation and deletion transaction IDs
- Implements garbage collection of obsolete versions
- Optimizes storage of versions

### Visibility Layer

- Implements rules to determine which versions are visible to each transaction
- Based on transaction ID, timestamp, and isolation level
- Ensures consistent views of the database
- Prevents anomalies like dirty reads, non-repeatable reads, and phantom reads

## Version Tracking

### Row Versioning

Each row in Stoolap can have multiple versions, with each version containing:

- **CreatedTxnID** - The transaction ID that created this version
- **DeletedTxnID** - The transaction ID that deleted this version (if any)
- **Column Values** - The actual data for this version

### Version Chain

Versions of a row form a chain:

1. New versions are created when rows are updated or deleted
2. Each version points to its predecessor
3. The chain represents the complete history of a row
4. Obsolete versions are eventually garbage collected

## Transaction IDs and Timestamps

Stoolap uses a sophisticated transaction ID and timestamp system:

- **TxnID** - Unique identifier for each transaction
- **ReadTimestamp** - Timestamp when the transaction started
- **CommitTimestamp** - Timestamp when the transaction committed

These identifiers are used to implement visibility rules and ensure transaction isolation.

## Isolation Levels

Stoolap supports two isolation levels through its MVCC implementation:

### Read Committed

- A transaction sees only committed data as of the start of each statement
- Different statements within the same transaction may see different data
- No dirty reads, but non-repeatable reads and phantom reads are possible
- Implemented through per-statement visibility checks

### Snapshot Isolation

- A transaction sees a consistent snapshot of the database as it existed at the start of the transaction
- All statements within a transaction see the same data, regardless of concurrent commits
- No dirty reads, non-repeatable reads, or phantom reads
- Implemented by using the transaction's read timestamp for all visibility checks

## Visibility Rules

The core of the MVCC implementation is the set of visibility rules:

### Basic Rules

A row version is visible to a transaction if:

1. It was created by the transaction itself, OR
2. It was created by a committed transaction before the current statement/transaction started, AND
3. It was not deleted, OR it was deleted by a transaction that had not committed when the current statement/transaction started, OR it was deleted by the current transaction

### Implementation

These rules are implemented in the `IsVisible` and `IsDirectlyVisible` functions:

```go
// Simplified example of visibility rules
func IsVisible(txn *Transaction, createdTxnID, deletedTxnID TxnID) bool {
    // Case 1: Row created by this transaction
    if createdTxnID == txn.ID {
        return true
    }
    
    // Case 2: Row created by another transaction
    creatorTxn := registry.GetTransaction(createdTxnID)
    if creatorTxn == nil || creatorTxn.Status != Committed || 
       creatorTxn.CommitTimestamp > txn.ReadTimestamp {
        return false
    }
    
    // Case 3: Row not deleted or deleted by uncommitted transaction
    if deletedTxnID == InvalidTxnID {
        return true
    }
    
    // Case 4: Row deleted by this transaction
    if deletedTxnID == txn.ID {
        return false
    }
    
    // Case 5: Row deleted by another transaction
    deleterTxn := registry.GetTransaction(deletedTxnID)
    return deleterTxn == nil || deleterTxn.Status != Committed || 
           deleterTxn.CommitTimestamp > txn.ReadTimestamp
}
```

## Concurrency Control

Stoolap uses optimistic concurrency control within its MVCC implementation:

1. Transactions proceed without acquiring locks for reading
2. Write operations create new versions without blocking readers
3. At commit time, the system checks for conflicts
4. If a conflict is detected, the transaction is aborted

### First-Committer-Wins

Stoolap implements the "first-committer-wins" strategy:

- If two transactions modify the same row, the first to commit succeeds
- The second transaction will detect the conflict at commit time and abort
- This approach minimizes blocking while ensuring data consistency

## Transaction Operations

### Beginning a Transaction

When a transaction begins:

1. A unique transaction ID is assigned
2. A read timestamp is captured
3. The transaction is registered in the transaction manager
4. The isolation level is set (ReadCommitted or SnapshotIsolation)

### Read Operations

During read operations:

1. The storage engine retrieves all versions of matching rows
2. The visibility layer filters versions based on the transaction's view
3. Only visible versions are returned
4. For ReadCommitted, visibility is based on the current statement time
5. For SnapshotIsolation, visibility is based on the transaction's start time

### Write Operations

During write operations:

1. **INSERT** - A new row version is created with the current transaction ID
2. **UPDATE** - A new row version is created, and the old version is marked as deleted
3. **DELETE** - The row version is marked as deleted by the current transaction
4. All changes are only visible to the current transaction until commit

### Committing a Transaction

When a transaction commits:

1. Conflict detection is performed
2. If conflicts are found, the transaction is aborted
3. If no conflicts, a commit timestamp is assigned
4. The transaction state is changed to Committed
5. Changes become visible to other transactions based on their isolation level

### Rolling Back a Transaction

When a transaction is rolled back:

1. All changes made by the transaction are discarded
2. The transaction state is changed to Aborted
3. Resources associated with the transaction are released

## Garbage Collection

To prevent unbounded growth of version chains:

1. Obsolete versions are identified (no active transaction can see them)
2. These versions are removed during garbage collection
3. Garbage collection runs periodically or when certain thresholds are reached
4. The oldest active transaction determines which versions can be safely removed

## Snapshot Management

For persistent storage, Stoolap manages consistent snapshots:

1. Snapshots represent a point-in-time state of the database
2. Each snapshot is consistent across all tables
3. Snapshots include only committed transactions
4. Recovery uses snapshots and WAL to rebuild the database state

## Implementation Details

Stoolap's MVCC is implemented in the following key components:

- **mvcc.go** - Core MVCC functionality and timestamp generation
- **transaction.go** - Transaction implementation
- **version_store.go** - Version storage and management
- **registry.go** - Transaction registry and visibility rules
- **table.go** - Table implementation with MVCC support
- **scanner.go** - Data scanning with visibility filtering

## Performance Optimizations

Several optimizations improve MVCC performance:

### Fast-Path Operations

- Single-transaction operations take a faster path
- Read-only transactions avoid certain overhead
- Common patterns are recognized and optimized

### Efficient Version Storage

- Versions are stored in a way that optimizes memory usage
- Column-oriented storage works well with MVCC
- Segment-based organization improves concurrency

### Visibility Filtering

- Batch filtering of versions improves performance
- Early pruning eliminates unnecessary version checks
- Index-based filtering accelerates visibility determination

## Best Practices

To get the most out of Stoolap's MVCC implementation:

1. **Keep transactions short** - Long-running transactions increase version accumulation
2. **Choose appropriate isolation levels** - Use the minimum required isolation
3. **Consider workload patterns** - Read-heavy workloads benefit most from MVCC
4. **Batch related operations** - Group related changes in a single transaction
5. **Handle conflicts appropriately** - Implement retry logic for conflicting transactions