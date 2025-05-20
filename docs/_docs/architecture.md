---
layout: doc
title: Stoolap Architecture
description: High-level overview of Stoolap's architecture and major components
permalink: /docs/architecture/
---

# Stoolap Architecture

This document provides a high-level overview of Stoolap's architecture, including its major components and how they interact.

## System Overview

Stoolap is a high-performance, column-oriented database engine designed for analytical workloads while also supporting transactional operations. Its architecture prioritizes:

- Memory-first design with optional disk persistence
- Columnar storage for efficient analytical queries
- Multi-version concurrency control (MVCC) for transaction isolation
- Vectorized execution for performance
- Zero external dependencies

## Core Components

Stoolap's architecture consists of the following major components:

### Client Interface

- **SQL Driver** - Standard database/sql driver implementation
- **Command-Line Interface** - Interactive CLI for database operations
- **API Layer** - Programmatic access to database functionality

### Query Processing Pipeline

1. **Parser** - Converts SQL text into an abstract syntax tree (AST)
   - Lexical analyzer (lexer.go)
   - Syntax parser (parser.go)
   - AST builder (ast.go)

2. **Planner/Optimizer** - Converts AST into an optimized execution plan
   - Plan generation
   - Statistics-based optimization
   - Expression optimization
   - Join order optimization

3. **Executor** - Executes the plan and produces results
   - Standard execution engine (executor.go)
   - Vectorized execution engine (vectorized/)
   - Query cache (query_cache.go)

### Storage Engine

- **MVCC Engine** - Multi-version concurrency control for transaction isolation
  - Transaction management (transaction.go)
  - Version store (version_store.go)
  - Visibility rules (registry.go)

- **Table Management** - Table creation and schema handling
  - Schema validation
  - Table metadata management
  - Column type management

- **Columnar Storage** - Column-oriented data organization
  - Column compression (compression/)
  - Type-specific storage optimizations
  - Segment-based organization

- **Indexing System** - Multiple index types for different access patterns
  - B-tree indexes (btree/)
  - Bitmap indexes (bitmap/)
  - Columnar indexes (mvcc/columnar_index.go)
  - Multi-column indexes (mvcc/columnar_index_multi.go)

- **Persistence Layer** - Optional disk storage
  - Binary serialization (binser/)
  - Snapshot management
  - Write-ahead logging (wal_manager.go)

### Function System

- **Function Registry** - Central registry for all SQL functions
  - Scalar functions (scalar/)
  - Aggregate functions (aggregate/)
  - Window functions (window/)

### Memory Management

- **Buffer Pool** - Reusable memory buffers to reduce allocation overhead
- **Value Pool** - Specialized object pooling for common data types
- **Segment Maps** - Efficient concurrent data structures

## Request Flow

When a query is executed, it flows through the system as follows:

1. **Query Submission**
   - SQL text is submitted via driver, CLI, or API

2. **Parsing and Validation**
   - SQL is parsed into an AST
   - Syntax and semantic validation is performed
   - Query is prepared for execution

3. **Planning and Optimization**
   - Execution plan is generated
   - Statistics are used to optimize the plan
   - Indexes are selected based on query patterns
   - Expression pushdown is applied where possible

4. **Execution**
   - For read queries:
     - Appropriate isolation level is applied
     - Storage engine provides data with visibility rules
     - Filters and projections are applied
     - Results are processed (joins, aggregations, sorting)
     - Final result set is returned

   - For write queries:
     - Transaction is started if not already active
     - Write operations are applied with MVCC rules
     - Indexes are updated
     - Changes are committed or rolled back

5. **Result Handling**
   - Results are formatted and returned to the client
   - Memory is released
   - Transaction state is updated

## Physical Architecture

### In-Memory Mode

In memory-only mode, Stoolap operates entirely in RAM:

- All data structures reside in memory
- No disk I/O for data access
- Highest performance but no durability

### Persistent Mode

In persistent mode, Stoolap uses disk storage with memory caching:

- Data is stored on disk in a specialized binary format
- Write-ahead logging ensures durability
- Memory serves as a cache for active data
- Background processes manage snapshots and cleanup

## Concurrency Model

Stoolap uses a combination of concurrency techniques:

- **MVCC** - Multiple versions of data for transaction isolation
- **Optimistic Concurrency Control** - Transactions validate at commit time
- **Fine-grained Locking** - Minimal contention through targeted locks
- **Segmented Data Structures** - Reduced lock contention
- **Lock-free Algorithms** - Where appropriate for performance

## Memory Efficiency

Several techniques are used to minimize memory usage:

- **Columnar Compression** - Type-specific compression algorithms
- **Memory Pooling** - Reuse of memory allocations
- **Reference Counting** - Efficient resource management
- **SIMD Operations** - Processing multiple values with single instructions

## Implementation Details

The core implementation is organized as follows:

- `/cmd/stoolap/` - Command-line interface
- `/pkg/` - Public API and driver implementation
- `/internal/` - Core implementation details
  - `/internal/parser/` - SQL parsing
  - `/internal/sql/` - SQL execution
  - `/internal/storage/` - Storage engine
  - `/internal/functions/` - SQL function implementations
  - `/internal/common/` - Common utilities
  - `/internal/fastmap/` - High-performance data structures

## Architectural Principles

Stoolap's architecture is guided by the following principles:

1. **Performance First** - Optimize for speed and memory efficiency
2. **Zero Dependencies** - Rely only on the Go standard library
3. **Modularity** - Clean component interfaces for extensibility
4. **Simplicity** - Favor simple solutions over complex ones
5. **Data Integrity** - Ensure consistent and correct results