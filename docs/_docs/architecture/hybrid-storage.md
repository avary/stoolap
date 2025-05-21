---
title: Hybrid Storage with Columnar Indexing
category: Architecture
order: 1
---

# Hybrid Storage with Columnar Indexing

This document explains Stoolap's hybrid storage architecture, which combines row-based storage with columnar indexing to achieve both transactional and analytical capabilities.

## Stoolap's HTAP Storage Approach

Stoolap takes a hybrid approach to data storage:

- **Row-based Version Store** - Primary storage for transactional operations
- **Columnar Indexing** - Optimized for analytical queries and scans

This hybrid approach is the foundation of Stoolap's HTAP (Hybrid Transactional/Analytical Processing) capabilities, providing efficient support for both OLTP and OLAP workloads.

## Row-Based Storage for OLTP

For transactional operations, Stoolap uses a row-based storage design:

### Advantages for Transactional Workloads

- **Efficient Record Access** - All fields of a record are stored together, optimizing point lookups
- **Low-latency Updates** - Faster for small, targeted modifications
- **Transaction Efficiency** - Better suited for ACID transaction processing
- **Write Optimization** - More efficient for inserting complete records

### Implementation

Stoolap's row-based storage consists of:

- **Version Store** - Tracks row versions for MVCC
- **Transaction Management** - Ensures ACID properties
- **Row Layout** - Optimized binary format for efficient access
- **In-Memory Tables** - Primary working set kept in memory
- **Disk Persistence** - Optional row-based on-disk format

## Columnar Indexing for OLAP

For analytical operations, Stoolap uses columnar indexing:

### Advantages for Analytical Workloads

- **Reduced Data Access** - Only required columns are processed
- **Efficient Filtering** - Column-oriented indexes optimize where clauses
- **Better Compression** - Similar data types compress more efficiently
- **Vectorized Processing** - Enables efficient batch operations

### Implementation

Stoolap's columnar indexing consists of:

- **Column Indexes** - Separate indexes for each column
- **Multi-Column Indexes** - Combined indexes for common query patterns
- **Expression Indexes** - Support for complex filtering conditions
- **Bitmap Indexes** - Efficient for low-cardinality columns

## HTAP Architecture Integration

The integration of row-based storage and columnar indexing provides several benefits:

### Unified Data Model

- Same data is accessible to both transactional and analytical workloads
- No need for ETL processes or data duplication
- Real-time analytics on live transactional data

### Query Routing

Stoolap's query optimizer can route queries to the appropriate storage structure:

- **Row Access** - For point lookups and small range scans
- **Column Access** - For large scans, aggregations, and complex filtering

### Consistency

- All data views remain consistent through the MVCC mechanism
- Analytical queries see a transactionally consistent snapshot
- No synchronization delay between transactional and analytical views

## Implementation Details

Key components implementing this architecture:

- **mvcc/table.go** - The table implementation with row-based storage
- **mvcc/version_store.go** - Manages row versions and MVCC
- **mvcc/columnar_index.go** - Single-column indexing implementation
- **mvcc/columnar_index_multi.go** - Multi-column indexing implementation
- **bitmap/index.go** - Bitmap indexing for efficient filtering
- **storage/expression/** - Expression-based filtering pushed down to storage

## Optimizations

Several optimizations improve performance across both storage paradigms:

### For Row Operations

- **Optimistic Concurrency Control** - Reduces locking overhead
- **Transaction Batching** - Processes multiple operations at once
- **Efficient Version Chain** - Optimized layout for version traversal

### For Columnar Operations

- **Predicate Pushdown** - Filters applied at the index level
- **Vectorized Processing** - Batch operations on indexed columns
- **SIMD Acceleration** - CPU-level parallelism for column operations
- **Bitmap-Based Filtering** - Fast set operations for result selection

## Performance Considerations

### OLTP Performance

- **Point Lookups** - Extremely fast due to row-based storage
- **Small Transactions** - Low overhead for common operations
- **High Concurrency** - Efficient handling of many simultaneous transactions

### OLAP Performance

- **Analytical Queries** - Efficient thanks to columnar indexing
- **Aggregations** - Optimized through vectorized processing
- **Complex Filtering** - Accelerated by bitmap and expression indexes

## Best Practices

To get the most out of Stoolap's hybrid architecture:

1. **Index Design** - Create columnar indexes on frequently filtered or aggregated columns
2. **Query Structure** - Use explicit column lists rather than SELECT *
3. **Transaction Sizing** - Keep transactions appropriately sized
4. **Query Planning** - Understand how the optimizer chooses between row and column access
5. **Data Types** - Use appropriate data types for better index performance