---
title: Transaction Isolation
category: Architecture
order: 1
---

# Transaction Isolation in Stoolap

Stoolap implements Multi-Version Concurrency Control (MVCC) to provide transaction isolation with minimal locking overhead. This document explains how transactions work in Stoolap and the available isolation levels.

## Implemented Isolation Levels

Stoolap currently supports two isolation levels:

1. **ReadCommitted** - The default isolation level where a transaction only sees data that has been committed by other transactions. Each query within the transaction may see different versions of data as other transactions commit.

2. **SnapshotIsolation** - Provides a consistent view of the database as it was when the transaction started. All queries within the transaction see the same snapshot of data, regardless of concurrent commits by other transactions.

## MVCC Implementation

Stoolap's MVCC implementation uses a combination of techniques to provide efficient transaction isolation:

### Transaction IDs and Timestamps

- Each transaction is assigned a unique transaction ID (`txnID`)
- Begin timestamps are assigned when the transaction starts
- Commit timestamps are assigned when the transaction commits
- Timestamps are used to determine data visibility between transactions

### Row Versioning

- Each row in a table can have multiple versions identified by transaction IDs
- Each version includes:
  - Creation transaction ID
  - Deletion transaction ID (if applicable)
  - Column values at that version
- This enables concurrent transactions to see different versions of the same row

### Visibility Rules

The core visibility rules in Stoolap determine which row versions are visible to a transaction:

#### ReadCommitted Mode

In ReadCommitted mode (the default):
- A transaction sees its own uncommitted changes
- A transaction sees all changes committed by other transactions at the time of each query
- A transaction never sees uncommitted changes from other transactions

#### SnapshotIsolation Mode

In SnapshotIsolation mode:
- A transaction sees its own uncommitted changes
- A transaction sees only changes that were committed before the transaction started
- Changes committed by other transactions after this transaction started are not visible

## Transaction Operations

### Beginning a Transaction

A transaction is started with the `BEGIN` SQL statement or implicitly by the first statement execution:

```sql
-- Start a transaction with default isolation level (ReadCommitted)
BEGIN;
```

Currently, specifying isolation levels directly in SQL statements (e.g., `BEGIN ISOLATION LEVEL SNAPSHOT`) is not supported. The isolation level is set at the engine level and applies to all transactions.

### Performing Operations

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
3. The transaction is removed from the active transactions list

### Rolling Back a Transaction

Transactions can be rolled back with the `ROLLBACK` statement:

```sql
ROLLBACK;
```

During rollback:
1. All changes made by the transaction are discarded
2. The transaction is removed from the active transactions list

## Concurrency Control

Stoolap uses optimistic concurrency control:

1. Transactions proceed without acquiring locks for reading
2. At commit time, the system checks for conflicts
3. If a conflict is detected, the transaction is aborted

### Conflict Detection

Conflicts are detected when two transactions attempt to modify the same row:

- **Primary Key Conflicts** - Occur when two transactions try to insert rows with the same primary key
- **Unique Constraint Conflicts** - Occur when an insert or update violates a unique constraint

## Implementation Details

The transaction isolation in Stoolap is implemented in the following key components:

- `engine.go` - Contains the transaction creation and management logic
- `registry.go` - Manages transaction metadata and visibility rules
- `transaction.go` - Implements the transaction interface
- `version_store.go` - Manages versioned data

## Best Practices

- Transactions should be kept short to minimize conflicts
- Use explicit transactions for operations that need to be atomic
- Consider your specific application's needs when deciding between ReadCommitted and SnapshotIsolation:
  - **ReadCommitted** is good for most OLTP workloads, offering better concurrency
  - **SnapshotIsolation** is useful for analytical queries that need a consistent point-in-time view

## Examples

### Basic Transaction

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### Transaction with Rollback

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
-- Something went wrong
ROLLBACK;
```

## Limitations

- Long-running transactions can cause version storage to grow, impacting performance
- The current implementation doesn't support explicitly setting isolation levels via SQL
- Extremely high concurrency on the same rows may lead to frequent conflicts
- Savepoints are not currently supported