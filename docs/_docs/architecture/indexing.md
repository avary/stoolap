---
layout: doc
title: Indexing
description: How indexes work in Stoolap and when to use them
permalink: /docs/indexing/
---

# Indexing in Stoolap

This document explains Stoolap's indexing system, including the types of indexes available, when to use each type, and best practices for index management.

## Index Types

Stoolap supports several types of indexes, each optimized for different query patterns:

### 1. Columnar Indexes

Columnar indexes are Stoolap's primary index type, optimized for column-oriented storage:

- **Design**: Maps column values to row positions
- **Strengths**: Fast range scans and equality lookups
- **Use Cases**: WHERE clause filtering, range queries, sort operations
- **Implementation**: Implemented in `columnar_index.go`

```sql
-- Create a columnar index
CREATE INDEX idx_user_signup ON users (signup_date);
```

### 2. Multi-Column Indexes

Multi-column indexes support filtering on multiple columns simultaneously:

- **Design**: Maps combinations of column values to row positions
- **Strengths**: Combined column filters, complex predicates
- **Use Cases**: Queries with multiple WHERE conditions
- **Implementation**: Implemented in `columnar_index_multi.go`

```sql
-- Create a multi-column index
CREATE INDEX idx_product_category_price ON products (category_id, price);
```

### 3. B-tree Indexes

B-tree indexes are traditional balanced tree structures:

- **Design**: Balanced tree structure for efficient lookups
- **Strengths**: General-purpose indexing with good performance
- **Use Cases**: Primary keys, unique constraints, range queries
- **Implementation**: Implemented in `btree/index.go` and `btree/btree.go`

```sql
-- Create a B-tree index (default index type)
CREATE INDEX idx_order_date ON orders (order_date);
```

### 4. Bitmap Indexes

Bitmap indexes use bit arrays for efficient filtering:

- **Design**: Bit vectors representing presence/absence of values
- **Strengths**: Low cardinality columns, boolean flags
- **Use Cases**: Filtering on status fields, boolean columns, enums
- **Implementation**: Implemented in `bitmap/index.go` and `bitmap/bitmap.go`

```sql
-- Create a bitmap index
CREATE BITMAP INDEX idx_product_status ON products (status);
```

## Index Selection and Usage

### Creating Indexes

Indexes are created using standard SQL syntax:

```sql
-- Basic index creation
CREATE INDEX index_name ON table_name (column_name);

-- Multiple column index
CREATE INDEX index_name ON table_name (column1, column2, column3);

-- Unique index
CREATE UNIQUE INDEX index_name ON table_name (column_name);

-- Bitmap index (explicitly specified)
CREATE BITMAP INDEX index_name ON table_name (column_name);
```

### When Stoolap Uses Indexes

The query optimizer considers indexes for:

1. **WHERE Clause Filtering** - Matching rows based on column values
2. **JOIN Operations** - Finding matching rows between tables
3. **ORDER BY** - Avoiding sort operations
4. **GROUP BY** - Pre-sorting data for group operations
5. **UNIQUE Constraints** - Enforcing uniqueness

### Index Selection Factors

The optimizer considers several factors when choosing indexes:

- **Cardinality** - Number of unique values in the indexed columns
- **Selectivity** - Percentage of rows that match the filter
- **Index Type** - Different index types excel at different operations
- **Available Statistics** - Data distribution information
- **Query Complexity** - Join conditions, filter expressions
- **Predicate Types** - Equality, range, LIKE, etc.

## Index Implementation Details

### Columnar Index

The columnar index is specialized for Stoolap's columnar storage:

- Maps column values to positions efficiently
- Organizes values for fast range and equality searches
- Maintains version information for MVCC
- Implements specific optimizations for different data types
- Provides fast access paths for common query patterns

For complex filtering, Stoolap's columnar index uses:
- Sorted value lists for efficient binary search
- Bitmap results for combining multiple conditions
- SIMD operations for parallel processing

### Multi-Column Index

Multi-column indexes extend the columnar index concept:

- Maps combinations of column values to positions
- Maintains column order importance (leftmost principle)
- Supports partial matching on prefixes of the index
- Enables advanced filtering across multiple dimensions
- Optimizes storage based on value distribution

### Primary Keys and Unique Indexes

Primary keys and unique indexes provide additional guarantees:

- Enforce uniqueness constraints on columns
- Serve as the primary access path for the table
- Enable efficient lookups by key value
- Support foreign key relationships

## Index Management

### Showing Indexes

View existing indexes using the SHOW INDEXES command:

```sql
SHOW INDEXES FROM table_name;
```

### Dropping Indexes

Remove indexes with the DROP INDEX command:

```sql
DROP INDEX index_name;
```

### Index Statistics

Stoolap maintains statistics about indexes:

- Number of unique values (cardinality)
- Value distribution information
- Index size and memory usage
- Access patterns and effectiveness

## Indexing and MVCC

Stoolap's indexes are integrated with the MVCC system:

- Indexes track row visibility based on transaction information
- Index operations see consistent snapshots based on transaction isolation
- Insert/update/delete operations update indexes transactionally
- Index entries include version information

## Optimizations

Stoolap implements several index optimizations:

### SIMD-Accelerated Filtering

- Uses CPU SIMD instructions for parallel processing
- Significantly accelerates filtering operations
- Particularly effective for columnar indexes

### Index-Only Scans

- When all required data is in the index, table access is avoided
- Improves performance by reducing data access

### Covering Indexes

- Indexes that include all columns needed by a query
- Enable index-only scans for better performance

### Bloom Filters

- Probabilistic data structure to quickly determine if a value might exist
- Used to accelerate certain index operations

## Best Practices

### When to Create Indexes

Create indexes on:

1. **Primary key columns** - Always index primary keys
2. **Foreign key columns** - Improves join performance
3. **Columns in WHERE clauses** - Speeds up filtering
4. **Columns in JOIN conditions** - Accelerates joins
5. **Columns in ORDER BY** - Avoids sorting
6. **Columns in GROUP BY** - Improves aggregation

### Index Design Guidelines

For optimal performance:

1. **Create selective indexes** - Indexes on columns with many unique values
2. **Consider column order** - Place high-selectivity columns first in multi-column indexes
3. **Limit index count** - Don't over-index tables (increases write overhead)
4. **Monitor index usage** - Drop unused indexes
5. **Choose appropriate index types** - Match index types to query patterns
6. **Balance read/write performance** - Indexes speed up reads but slow down writes

### Common Indexing Mistakes

Avoid these common issues:

1. **Indexing low-cardinality columns alone** - Limited benefit
2. **Over-indexing** - Too many indexes hurt write performance
3. **Incorrect multi-column index order** - Reduces effectiveness
4. **Not indexing JOIN columns** - Leads to poor join performance
5. **Indexing very large text fields** - Consider partial indexing or function indexes

## Performance Considerations

### Write Impact

Indexes affect write operations:

- Each index requires additional updates during inserts/updates/deletes
- More indexes mean slower write operations
- Index maintenance consumes CPU and memory

### Space Requirements

Indexes increase storage requirements:

- Each index increases the database size
- Different index types have different space characteristics
- Consider space-performance tradeoffs

### Query Performance

Indexes dramatically improve query performance:

- Reduce the amount of data scanned
- Speed up filter operations
- Eliminate sorting for ORDER BY
- Accelerate join operations

## Implementation Notes

Stoolap's indexes are implemented in:

- `/internal/storage/mvcc/columnar_index.go` - Columnar index implementation
- `/internal/storage/mvcc/columnar_index_multi.go` - Multi-column index
- `/internal/storage/btree/index.go` - B-tree index
- `/internal/storage/bitmap/index.go` - Bitmap index
- `/internal/sql/executor/executor.go` - Index selection logic