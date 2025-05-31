---
layout: post
title: "Why NewSQL That Starts Simple Matters"
date: 2025-01-30
author: Semih Alev
excerpt: "NewSQL databases traditionally require complex distributed systems from day one. We took a different approach: start with simplicity, grow into sophistication. Here's how Stoolap brings enterprise database capabilities to embedded systems."
---

# Why NewSQL That Starts Simple Matters

NewSQL represents a fundamental shift in database architecture - combining ACID guarantees with horizontal scalability and modern performance characteristics. At Stoolap, we asked: why should this power require complexity from day one?

## Understanding NewSQL

NewSQL databases emerged to solve a critical challenge: maintaining transactional consistency while achieving the scale and performance of modern distributed systems. Key characteristics include:

- **ACID compliance** without sacrificing performance
- **Horizontal scalability** through innovative architectures
- **SQL compatibility** preserving existing knowledge
- **Modern concurrency control** like MVCC

## The Stoolap Approach: Embedded NewSQL

We've taken these enterprise-grade concepts and made them accessible in an embedded database:

### Multi-Version Concurrency Control (MVCC)

```go
// MVCC is built into every operation
db, _ := stoolap.Open("memory://")

// Concurrent transactions work naturally
go func() {
    tx, _ := db.Begin()
    tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 1")
    tx.Commit()
}()

go func() {
    tx, _ := db.Begin()
    tx.Exec("SELECT SUM(balance) FROM accounts")
    tx.Commit()
}()
```

Our MVCC implementation provides:
- **Lock-free reads** - Readers never wait for writers
- **Point-in-time consistency** - Each transaction sees a stable snapshot
- **Optimistic concurrency** - High throughput for mixed workloads

### Hybrid Transactional/Analytical Processing (HTAP)

Traditional databases optimize for either transactions or analytics. Stoolap handles both:

```sql
-- Morning: Transaction processing
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 123;
INSERT INTO orders (customer_id, product_id, amount) VALUES (456, 123, 99.99);
COMMIT;

-- Same table: Real-time analytics
CREATE COLUMNAR INDEX idx_analytics ON orders(amount, created_at);
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    SUM(amount) as revenue,
    COUNT(*) as order_count
FROM orders
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY hour;
```

### Transaction Isolation Levels

Stoolap implements proper isolation levels:

```sql
-- READ COMMITTED (default) - High concurrency
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
-- See committed changes immediately

-- SNAPSHOT - Consistent view
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
-- Stable view from transaction start
-- Write-write conflict detection
```

## Technical Implementation Details

### Version Store Architecture
```go
// Each row maintains version history
type RowVersion struct {
    TxnID          int64       // Creating transaction
    DeletedAtTxnID int64       // Deletion marker
    Data           storage.Row // Actual data
    prev           *RowVersion // Version chain
}
```

### Columnar Index Structure
- **Compressed storage** - Dictionary, RLE, and delta encoding
- **Vectorized execution** - SIMD operations for aggregations
- **Smart query routing** - Optimizer chooses row vs columnar path

### Pure Go Implementation
```go
// Zero CGO dependencies
// Single binary deployment
// Cross-platform by default
import "github.com/stoolap/stoolap"

db, _ := stoolap.Open("file:///data/myapp.db")
```

## Performance Characteristics

Our architecture delivers:

- **Microsecond latency** for point queries
- **Concurrent write throughput** via MVCC
- **Analytical query acceleration** through columnar indexes
- **Memory-efficient** version management
- **Automatic garbage collection** of old versions

## The Progressive Enhancement Model

Start simple:
```go
// Day 1: Basic embedded database
db.Exec("CREATE TABLE data (id INT, value TEXT)")
```

Add sophistication as needed:
```go
// When you need analytics
db.Exec("CREATE COLUMNAR INDEX FOR analytics")

// When you need isolation
db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSnapshot,
})
```

## Architecture Benefits

### Simplified Operations
- No cluster coordination on day one
- No configuration complexity
- No separate analytical database

### Consistent Performance
- Predictable query execution
- No network latency
- Local data access patterns

### Future-Ready Design
- Architecture supports distribution
- Clean separation of concerns
- Extensible storage engine

## Conclusion

NewSQL that starts simple isn't about compromising power for ease of use. It's about recognizing that sophisticated database capabilities shouldn't require operational complexity from the start. 

Stoolap brings enterprise-grade database technology - MVCC, HTAP, proper isolation levels - to the embedded database space. Start with a single file, grow to handle complex workloads, all with the same powerful engine.

The future of databases isn't about choosing between simple and powerful. It's about having both.

---

*Explore the [technical documentation](/docs) or see the [implementation details](https://github.com/stoolap/stoolap) to learn more about our NewSQL architecture.*