---
title: Transaction Isolation
category: Architecture
order: 1
---

# Transaction Isolation in Stoolap

Stoolap implements Multi-Version Concurrency Control (MVCC) to provide transaction isolation with minimal locking overhead. This document covers how transactions work in Stoolap, the available isolation levels, and best practices for transaction management.

## Isolation Levels

Stoolap supports two isolation levels:

1. **ReadCommitted** - Provides read committed isolation where transactions only see data that has been committed by other transactions at the time a statement begins execution.

2. **SnapshotIsolation** - Provides snapshot isolation where transactions see a consistent snapshot of the database as it existed at the start of the transaction.

## MVCC Implementation

Stoolap's MVCC implementation uses a combination of techniques to provide efficient transaction isolation:

### Transaction IDs and Timestamps

- Each transaction is assigned a unique transaction ID (`TxnID`)
- Read timestamps are assigned when the transaction starts
- Commit timestamps are assigned when the transaction commits
- Timestamps are used to determine data visibility between transactions

### Row Versioning

- Each row in a table can have multiple versions identified by transaction IDs
- Each version includes:
  - Creation transaction ID (`CreatedTxnID`)
  - Deletion transaction ID (`DeletedTxnID`, if applicable)
  - Column values at that version
- This enables concurrent transactions to see different versions of the same row

### Visibility Rules

Visibility rules determine which row versions are visible to a transaction. These rules are based on:

- Transaction's isolation level (ReadCommitted or SnapshotIsolation)
- Transaction's read timestamp
- Transaction IDs of row versions (creation and deletion)
- Commit state of other transactions

Core visibility principles:
1. A transaction sees its own changes
2. A transaction sees committed changes from other transactions based on its isolation level
3. A transaction never sees uncommitted changes from other transactions (no dirty reads)

## Transaction Lifecycle

### Starting a Transaction

A transaction is started with the `BEGIN` SQL statement or implicitly when the first statement is executed. You can specify the isolation level:

```sql
-- Start a transaction with read committed isolation (default)
BEGIN;

-- Start a transaction with snapshot isolation
BEGIN ISOLATION LEVEL SNAPSHOT;
```

### Transaction Operations

Within a transaction, you can perform the following operations:

- **SELECT** - Reads data visible to the transaction
- **INSERT** - Creates new row versions
- **UPDATE** - Creates new row versions and marks old versions as deleted
- **DELETE** - Marks row versions as deleted

### Committing a Transaction

Transactions are committed with the `COMMIT` statement:

```sql
COMMIT;
```

During commit:
1. A commit timestamp is assigned to the transaction
2. All changes become visible to other transactions (based on their isolation level)
3. All locks held by the transaction are released

### Rolling Back a Transaction

Transactions can be rolled back with the `ROLLBACK` statement:

```sql
ROLLBACK;
```

During rollback:
1. All changes made by the transaction are discarded
2. All locks held by the transaction are released

## Concurrency Control

Stoolap uses optimistic concurrency control:

1. Transactions proceed without acquiring locks for reading
2. At commit time, the system checks for conflicts
3. If a conflict is detected, the transaction is aborted

### Conflict Resolution

Conflicts are detected when two transactions attempt to modify the same row:

- **First-Committer-Wins** - If two transactions modify the same row, the first to commit succeeds, and the second receives an error
- **Aborted Transactions** - If a conflict is detected, the transaction is automatically aborted and must be retried

## Performance Considerations

Stoolap's MVCC implementation includes several optimizations:

- **In-memory Version Storage** - Row versions are stored in memory for fast access
- **Efficient Version Cleaning** - Old versions are cleaned up when no longer needed
- **Column-based Storage** - Data is stored in columnar format for efficient version management

## Best Practices

- Use the appropriate isolation level for your use case:
  - **ReadCommitted** for higher concurrency with acceptable consistency
  - **SnapshotIsolation** for stronger consistency with potentially lower concurrency
- Keep transactions short to minimize conflicts
- Handle transaction aborts with appropriate retry logic
- Consider using explicit transactions for related operations that need to be atomic

## Limitations

- Long-running transactions can cause version storage to grow, impacting performance
- Extremely high concurrency on the same rows may lead to frequent conflicts, requiring retry logic

## Practical Examples

### Example 1: Basic Transaction with ReadCommitted

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### Example 2: Snapshot Isolation for Consistent Reads

```sql
BEGIN ISOLATION LEVEL SNAPSHOT;
-- All reads in this transaction will see a consistent snapshot
-- even if other transactions commit changes
SELECT * FROM accounts WHERE balance > 1000;
-- ... other operations
COMMIT;
```

### Example 3: Handling Conflicts

```sql
-- Application code with retry logic
function transferMoney() {
    let retries = 0;
    while (retries < MAX_RETRIES) {
        try {
            execute("BEGIN");
            execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
            execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
            execute("COMMIT");
            return SUCCESS;
        } catch (e) {
            execute("ROLLBACK");
            retries++;
        }
    }
    return FAILURE;
}
```

## Implementation Details

Stoolap's transaction isolation is implemented in the following key components:

- `transaction.go` - Core transaction implementation
- `mvcc.go` - MVCC engine and timestamp generation
- `version_store.go` - Version storage and management
- `registry.go` - Transaction registry for tracking transaction states