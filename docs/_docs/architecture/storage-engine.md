---
layout: doc
title: Storage Engine
description: How data is stored and retrieved in Stoolap
permalink: /docs/storage-engine/
---

# Storage Engine

This document provides a detailed overview of Stoolap's storage engine, including its design principles, components, and how data is stored and retrieved.

## Storage Engine Design

Stoolap's storage engine is designed with the following principles:

- **Column-oriented** - Data is organized by column rather than by row
- **Memory-optimized** - Prioritizes in-memory performance with optional persistence
- **MVCC-based** - Uses multi-version concurrency control for transaction isolation
- **Segment-organized** - Data is divided into segments for efficient management
- **Type-specialized** - Uses different strategies for different data types
- **Index-accelerated** - Multiple index types to optimize different query patterns

## Storage Components

### Table Structure

Tables in Stoolap are composed of:

- **Metadata** - Schema information, column definitions, and indexes
- **Columns** - The actual data storage, organized by column
- **Segments** - Data is divided into segments for better management
- **Indexes** - Optional structures for accelerating queries
- **Version Store** - Tracks row versions for MVCC

### Column Types

Each column type has a specialized implementation:

- **Int64Column** - Optimized for 64-bit integers
- **Float64Column** - Optimized for 64-bit floating-point numbers
- **StringColumn** - Optimized for variable-length string data
- **BoolColumn** - Optimized for boolean values
- **TimestampColumn** - Optimized for datetime values
- **JSONColumn** - Optimized for JSON documents
- **NullableColumn** - Wrapper that adds NULL support to any column type

### Segment Management

Data in each table is divided into segments:

- New data is added to the active segment
- When a segment reaches a certain size, a new active segment is created
- Each segment is managed independently
- This approach improves concurrency and memory management

## Data Storage Format

### In-Memory Format

In memory, data is stored in specialized column structures:

- **Fixed-width types** (INT, FLOAT, BOOL) - Stored in arrays or slices
- **Variable-width types** (STRING, JSON) - Stored with dictionaries or offset arrays
- **Nullable types** - Paired with bitmap for NULL indicators
- **Versions** - Associated with transaction IDs for MVCC

### On-Disk Format

When persistence is enabled, data is stored on disk with:

- **Binary serialization** - Compact binary format for storage
- **Column files** - Separate files for each column
- **Metadata files** - Schema and index information
- **WAL files** - Write-ahead log for durability
- **Snapshot files** - Point-in-time table snapshots

## MVCC Implementation

The storage engine uses MVCC to provide transaction isolation:

- **Row Versioning** - Multiple versions of each row are maintained
- **Transaction IDs** - Each version is associated with a transaction ID
- **Visibility Rules** - Determine which versions are visible to each transaction
- **Garbage Collection** - Old versions are cleaned up when no longer needed

For more details, see the [MVCC Implementation](/docs/mvcc-implementation/) and [Transaction Isolation](/docs/transaction-isolation/) documentation.

## Data Access Paths

### Table Scan

For full table scans:

1. The storage engine identifies visible segments
2. Each segment is scanned in sequence or parallel
3. Visibility rules are applied to filter versions
4. Columns are accessed directly for projection
5. Batch processing is used for vectorized execution

### Index Access

For index-based access:

1. The appropriate index is selected
2. The index is used to identify candidate rows
3. Visibility rules are applied to filter versions
4. Remaining predicates are evaluated if needed
5. Columns are accessed for projection

## Data Modification

### Insert Operations

When data is inserted:

1. Values are validated against column types
2. A new row version is created in the active segment
3. The row is marked with the current transaction ID
4. Indexes are updated to include the new row
5. The operation is recorded in the WAL (if enabled)

### Update Operations

When data is updated:

1. The existing row versions are located
2. Visibility rules determine which versions to update
3. New row versions are created with updated values
4. Old versions are marked as deleted by the current transaction
5. Indexes are updated to reflect the changes
6. The operation is recorded in the WAL (if enabled)

### Delete Operations

When data is deleted:

1. The existing row versions are located
2. Visibility rules determine which versions to delete
3. Matched versions are marked as deleted by the current transaction
4. Indexes are updated to reflect the deletions
5. The operation is recorded in the WAL (if enabled)

## Persistence and Recovery

When persistence is enabled:

### Write-Ahead Logging (WAL)

1. All modifications are recorded in the WAL before being applied
2. WAL entries include transaction ID, operation type, and data
3. WAL is flushed to disk based on sync mode configuration
4. This ensures durability in case of crashes

### Snapshots

1. Periodically, consistent snapshots of tables are created
2. Snapshots represent a point-in-time state of each table
3. Multiple snapshots may be retained for safety
4. Snapshots accelerate recovery compared to replaying the entire WAL

### Recovery Process

After a crash, recovery proceeds as follows:

1. The latest valid snapshot is loaded for each table
2. WAL entries after the snapshot are replayed
3. Transaction state is reconstructed
4. Incomplete transactions are rolled back

## Memory Management

The storage engine includes several memory optimization techniques:

### Buffer Pool

- Reusable memory buffers reduce allocation overhead
- Buffers are managed in pools by size categories
- Used for both in-memory operations and disk I/O

### Value Pool

- Specialized object pooling for common data types
- Reduces garbage collection pressure
- Particularly beneficial for string and structural data

### Segment Maps

- Efficient concurrent data structures
- Optimized for different access patterns
- Specialized implementations for common key types (Int64)

## Implementation Details

Core storage engine components:

- **storage.go** - Storage interfaces and factory functions
- **column.go** - Column type implementations
- **mvcc/** - MVCC implementation components
  - **engine.go** - MVCC storage engine
  - **table.go** - Table implementation
  - **transaction.go** - Transaction management
  - **version_store.go** - Version tracking
  - **columnar_index.go** - Columnar indexing
  - **scanner.go** - Data scanning
  - **disk_version_store.go** - Persistence implementation
  - **wal_manager.go** - Write-ahead logging
- **compression/** - Column compression implementations
- **expression/** - Storage-level expression evaluation

## Performance Characteristics

- **Read Performance** - Columnar organization optimizes analytical queries
- **Write Performance** - Segment-based design with append-only writes
- **Concurrency** - MVCC enables high concurrent access
- **Memory Efficiency** - Type-specific optimizations reduce memory footprint
- **I/O Efficiency** - Batched and targeted I/O with compression