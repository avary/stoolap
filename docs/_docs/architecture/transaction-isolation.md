---
title: Transaction Isolation
category: Architecture
order: 1
---

# Transaction Isolation in Stoolap

Stoolap implements Multi-Version Concurrency Control (MVCC) to provide transaction isolation with minimal locking overhead. This document explains how transactions work in Stoolap and the available isolation levels.

## Implemented Isolation Levels

Stoolap currently supports two isolation levels:

1. **READ COMMITTED** - The default isolation level where a transaction only sees data that has been committed by other transactions. Each query within the transaction may see different versions of data as other transactions commit.

2. **SNAPSHOT** (also known as REPEATABLE READ) - Provides a consistent view of the database as it was when the transaction started. All queries within the transaction see the same snapshot of data, regardless of concurrent commits by other transactions.

## Setting Isolation Levels

Stoolap supports setting isolation levels both at the session level and per-transaction:

### Session-wide Isolation Level

Set the default isolation level for all transactions in the current session:

```sql
-- Set session isolation level to READ COMMITTED (default)
SET ISOLATIONLEVEL = 'READ COMMITTED';

-- Set session isolation level to SNAPSHOT
SET ISOLATIONLEVEL = 'SNAPSHOT';
```

### Transaction-specific Isolation Level

Override the session isolation level for a specific transaction:

```sql
-- Start a transaction with READ COMMITTED isolation level
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- Start a transaction with SNAPSHOT isolation level  
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
```

**Important**: When a transaction with a specific isolation level completes (via COMMIT or ROLLBACK), the isolation level automatically reverts to the original session-wide setting.

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

#### READ COMMITTED Mode

In READ COMMITTED mode (the default):
- A transaction sees its own uncommitted changes
- A transaction sees all changes committed by other transactions at the time of each query
- A transaction never sees uncommitted changes from other transactions

#### SNAPSHOT Mode

In SNAPSHOT mode:
- A transaction sees its own uncommitted changes
- A transaction sees only changes that were committed before the transaction started
- Changes committed by other transactions after this transaction started are not visible

## Transaction Operations

### Beginning a Transaction

A transaction can be started with various forms of the `BEGIN` statement:

```sql
-- Start a transaction with default isolation level (READ COMMITTED)
BEGIN;

-- Alternative syntax
BEGIN TRANSACTION;

-- Start with specific isolation level
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
```

Transactions can also start implicitly with the first statement execution if no explicit transaction is active.

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
4. If a transaction-specific isolation level was set, it reverts to the session default

### Rolling Back a Transaction

Transactions can be rolled back with the `ROLLBACK` statement:

```sql
ROLLBACK;
```

During rollback:
1. All changes made by the transaction are discarded
2. The transaction is removed from the active transactions list
3. If a transaction-specific isolation level was set, it reverts to the session default

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
- `transaction.go` - Implements the transaction interface with isolation level management
- `version_store.go` - Manages versioned data
- `executor.go` - Handles SQL statement execution and isolation level parsing

## Best Practices

- Transactions should be kept short to minimize conflicts
- Use explicit transactions for operations that need to be atomic
- Consider your specific application's needs when deciding between READ COMMITTED and SNAPSHOT:
  - **READ COMMITTED** is good for most OLTP workloads, offering better concurrency
  - **SNAPSHOT** is useful for analytical queries that need a consistent point-in-time view
- Use session-wide isolation levels for applications with consistent isolation requirements
- Use transaction-specific isolation levels when you need different isolation guarantees for specific operations

## Examples

### Basic Transaction with Default Isolation

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### Transaction with Specific Isolation Level

```sql
-- Use SNAPSHOT isolation for a consistent analytical query
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
SELECT category, SUM(amount) FROM sales GROUP BY category;
SELECT COUNT(*) FROM products WHERE active = true;
COMMIT;
```

### Session-wide Isolation Level Setting

```sql
-- Set session to use SNAPSHOT isolation by default
SET ISOLATIONLEVEL = 'SNAPSHOT';

-- All subsequent transactions will use SNAPSHOT isolation
BEGIN;
SELECT * FROM inventory WHERE quantity > 0;
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 123;
COMMIT;

-- Reset to default
SET ISOLATIONLEVEL = 'READ COMMITTED';
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
- Extremely high concurrency on the same rows may lead to frequent conflicts
- Savepoints are not currently supported
- The isolation level automatic restoration only works for properly completed transactions (COMMIT/ROLLBACK)