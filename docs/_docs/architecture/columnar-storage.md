---
layout: doc
title: Columnar Storage
description: Column-oriented storage architecture in Stoolap
permalink: /docs/columnar-storage/
---

# Columnar Storage

This document explains Stoolap's columnar storage architecture, its advantages for analytical workloads, and implementation details.

## What is Columnar Storage?

Traditional databases use row-oriented storage, where all fields of a record are stored together. In contrast, Stoolap uses column-oriented storage, where data for each column is stored separately. This approach offers significant advantages for analytical queries that typically process only a subset of columns.

## Advantages of Columnar Storage

### Analytical Query Performance

Columnar storage excels at analytical workloads:

- **Reduced I/O** - Only required columns are read from storage
- **Better Compression** - Similar data types compress more efficiently together
- **Vectorized Processing** - Enables efficient batch operations on columns
- **SIMD Operations** - Facilitates CPU SIMD instructions for parallel data processing

### Memory Efficiency

- **Type-Specific Compression** - Each column uses specialized compression
- **Reduced Memory Footprint** - Lower overall memory usage for large datasets
- **Cache Efficiency** - Better CPU cache utilization for sequential column access

### Operational Benefits

- **Schema Flexibility** - Adding or removing columns has minimal impact
- **Type-Specific Optimizations** - Each data type can use optimized algorithms
- **Simplified Vectorization** - Natural fit for vectorized execution engines

## Columnar Architecture in Stoolap

### Column Implementation

Each column in Stoolap is implemented as a separate data structure:

- **Data Array** - Stores the actual column values
- **NULL Bitmap** - Tracks which values are NULL (for nullable columns)
- **Deletion Map** - Tracks which rows are deleted (for MVCC)
- **Version Information** - Associates values with transaction IDs (for MVCC)

### Column Types

Each data type has a specialized column implementation:

1. **Int64Column**
   - Stores 64-bit integers
   - May use delta or bit-packing compression
   - Optimized for numerical operations

2. **Float64Column**
   - Stores 64-bit floating-point numbers
   - Uses specialized floating-point compression
   - Optimized for numerical operations

3. **StringColumn**
   - Stores variable-length string data
   - Often uses dictionary compression for repeated values
   - Maintains offset arrays for efficient access

4. **BoolColumn**
   - Stores boolean values
   - Highly compressed using bit arrays
   - Optimized for boolean operations

5. **TimestampColumn**
   - Stores date/time values
   - Uses specialized timestamp compression
   - Optimized for time-series operations

6. **JSONColumn**
   - Stores JSON documents
   - Uses JSON-specific compression
   - Provides path-based access operations

### Segment-Based Organization

Columns are further divided into segments:

- Each segment contains a portion of the column data
- New data is appended to the active segment
- When a segment reaches a certain size, a new segment is created
- This approach improves concurrency and memory management

### Nullability

NULL values are handled efficiently:

- A bitmap tracks which positions contain NULL values
- This separation avoids special "NULL" markers in the data arrays
- Operations can quickly skip NULL values

## Data Operations in Columnar Storage

### Reading Data

When executing a query, Stoolap:

1. Identifies which columns are needed based on the query
2. Applies visibility rules based on MVCC and transaction isolation
3. Reads only the required columns from memory or disk
4. Processes column data in batches for vectorized operations
5. Combines column values into rows only when necessary

### Writing Data

For write operations:

1. **INSERT** - New values are appended to each column in the active segment
2. **UPDATE** - New versions of affected columns are created
3. **DELETE** - Rows are marked as deleted in the version store

## Columnar Indexing

Stoolap implements specialized columnar indexes:

- **Single-Column Indexes** - Optimized for filtering on one column
- **Multi-Column Indexes** - Support complex filtering on multiple columns
- These indexes leverage the columnar organization for efficiency

For more details, see the [Indexing](/docs/indexing/) documentation.

## Compression in Columnar Storage

Each column type uses specialized compression techniques:

- **Dictionary Compression** - For columns with repeated values
- **Run-Length Encoding** - For columns with consecutive repeated values
- **Delta Encoding** - For columns with small differences between values
- **Bit-Packing** - For integer columns with values in a limited range
- **Time-Specific Compression** - For datetime and timestamp columns

For more details, see the [Data Compression](/docs/data-compression/) documentation.

## Columnar Storage and MVCC

The columnar storage architecture is integrated with MVCC:

1. Each value is associated with transaction information
2. Multiple versions of values are maintained
3. A visibility layer determines which versions are visible to each transaction
4. This enables efficient transactional guarantees without sacrificing columnar benefits

For more details, see the [MVCC Implementation](/docs/mvcc-implementation/) documentation.

## Vectorized Processing

Columnar storage naturally enables vectorized processing:

1. Data is processed in batches rather than row-by-row
2. Operations are applied to entire column segments at once
3. This reduces interpretation overhead and improves CPU cache utilization
4. SIMD instructions can process multiple values simultaneously

## Implementation Details

Stoolap's columnar storage is implemented in several key components:

- **column.go** - Base column interfaces and common functionality
- **mvcc/table.go** - Table implementation using columnar storage
- **mvcc/scanner.go** - Efficient scanning of columnar data
- **compression/** - Type-specific compression implementations
- **vectorized/** - Vectorized execution engine for columnar data

Each column implementation provides the following key operations:

- **Append** - Add new values to a column
- **Get** - Retrieve values at specific positions
- **Scan** - Iterate through column values
- **Filter** - Apply predicates to identify matching positions
- **Serialize/Deserialize** - Convert between memory and disk formats

## Performance Considerations

### Strengths

- **Analytical Queries** - Excels at aggregations, reports, and data analysis
- **Compression Efficiency** - Typically achieves higher compression ratios
- **Batch Processing** - Highly efficient for processing large amounts of data

### Considerations

- **Point Lookups** - Single-row lookups may require reading multiple columns
- **Small Transactions** - Small insert/update operations have some overhead
- **Memory Usage** - Complex queries might require more working memory

## Best Practices

To get the most out of Stoolap's columnar storage:

1. **Select only needed columns** - The primary benefit comes from reading only required data
2. **Use appropriate data types** - This maximizes compression and performance
3. **Batch operations when possible** - Insert/update in batches for better throughput
4. **Consider column ordering** - Frequently accessed columns may benefit from being defined first
5. **Leverage columnar indexes** - Create indexes on commonly filtered columns