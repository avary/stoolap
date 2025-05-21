---
title: Vectorized Execution
category: Performance
order: 1
---

# Vectorized Execution in Stoolap

## Overview

Stoolap's vectorized execution engine is designed to accelerate SQL query processing by operating on data in columnar batches rather than row-by-row, allowing for better CPU utilization and cache efficiency. This document explains the architecture and components of the vectorized execution system.

## Key Concepts

### Batch Processing

The vectorized execution model processes data in batches to improve memory locality and enable SIMD (Single Instruction, Multiple Data) operations. Instead of processing one row at a time, the engine processes multiple rows simultaneously using array-based operations.

Key characteristics:
- Default batch size: 32,768 rows (`MaxBatchSize = 1024 * 32`)
- Parallelization threshold: 5,000 rows (`ParallelThreshold`)
- Optimal processing block: 64 elements (`OptimalBlockSize`)

### Columnar Batch Structure

For efficient vector processing, data from the row-based storage is organized into columnar batches:

```go
type Batch struct {
    // Size is the number of rows in the batch
    Size int

    // Column arrays organized by data type for efficient processing
    IntColumns    map[string][]int64
    FloatColumns  map[string][]float64
    StringColumns map[string][]string
    BoolColumns   map[string][]bool
    TimeColumns   map[string][]time.Time

    // Null indicators for each column (true = NULL)
    NullBitmaps map[string][]bool

    // List of column names in the batch (for ordered processing)
    ColumnNames []string
}
```

### Memory Management

The engine uses object pools to minimize garbage collection overhead:
- Batch pool for reusing batch structures
- Type-specific buffer pools for column arrays
- Selection vector pool for filtered data operations

## Architecture Components

### 1. Vectorized Engine

The `Engine` struct in the `vectorized` package is the central component responsible for:
- Analyzing queries to determine if they can be vectorized
- Extracting table and column information
- Converting row-based input to columnar batches
- Applying vectorized operations (filtering, projection)
- Converting back to row-based results

```go
type Engine struct {
    // Dependencies
    functionRegistry contract.FunctionRegistry
}
```

### 2. Processors

Processors are composable components that operate on batches and transform them:

- **Comparison Processors**: Filter rows based on comparison conditions
  - `CompareConstantProcessor`: Compares a column to a constant value
  - `CompareColumnsProcessor`: Compares two columns
  - `SIMDComparisonProcessor`: Optimized numeric comparison using SIMD instructions

- **Logical Processors**: Combine multiple conditions
  - `AndProcessor`: Processes batches with AND logic
  - `OrProcessor`: Combines results with OR logic
  - `NotProcessor`: Inverts the result of another processor

- **Arithmetic Processors**: Perform mathematical operations on columns

### 3. SIMD Operations

The `simd.go` file contains optimized operations for vectorized processing:

- Basic Vector Operations:
  - Addition, subtraction, multiplication, division
  - Scalar-vector operations (add scalar to vector, multiply vector by scalar)
  - Comparison operations (equals, not equals, greater than, etc.)

- Statistical Functions:
  - Sum, mean, min/max

- Loop unrolling techniques:
```go
// Process 8 elements at a time with manual loop unrolling
i := 0
for ; i <= length-8; i += 8 {
    dst[i] = a[i] * b[i]
    dst[i+1] = a[i+1] * b[i+1]
    dst[i+2] = a[i+2] * b[i+2]
    // ...
}

// Handle remaining elements
for ; i < length; i++ {
    dst[i] = a[i] * b[i]
}
```

## Query Flow

1. **Query Analysis**: The engine analyzes the SQL query to determine if it's suitable for vectorized execution.

2. **Data Fetching**: The base data is fetched from the storage layer.

3. **Batch Creation**: The row-based data is converted to columnar batches.

4. **Filter Application**: WHERE clauses are applied using optimized vector operations.

5. **Expression Evaluation**: SELECT list expressions are processed in a vectorized manner.

6. **Result Formation**: The final batch is converted back to a row-based result that implements the `storage.Result` interface.

## Optimization Techniques

### SIMD-Compatible Operations

The engine detects operations that can benefit from SIMD optimization:
- Numeric comparisons (=, !=, <, >, etc.)
- Arithmetic operations (+, -, *, /)
- Special functions (sqrt, exp, log)

### Memory Locality

Operations are designed to maximize cache efficiency:
- Processing data in small blocks (typically 64 elements)
- Minimizing pointer chasing
- Sequential memory access patterns

### Expression Optimization

The engine analyzes expressions to determine optimal execution paths:
- Fast paths for common operations
- Optimized handling of constants
- Instruction-level parallelism with multiple accumulators

### Selection Vectors

Instead of materializing intermediate results for filtering operations, the engine uses selection vectors (arrays of row indices) to track which rows pass filters, reducing memory usage and copy operations.

## Performance Considerations

- Vectorized execution shows the greatest benefits for compute-intensive operations.
- For very small data sets, the overhead of vectorization might outweigh the benefits.
- Complex expressions involving many arithmetic operations benefit most from vectorization.
- SIMD operations work best on integer and floating-point data types.

## Limitations

Currently, the vectorized engine supports:
- Simple SELECT queries without:
  - Complex joins
  - Aggregations
  - Window functions
  - Subqueries

Future extensions will gradually add support for more complex operations in a vectorized manner.

## Usage Example

The vectorized engine is integrated with the SQL executor and can be used transparently for suitable queries:

```go
// Execute a SQL query with vectorization if possible
func executePotentiallyVectorized(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
    // Check if the query is suitable for vectorization
    if vectorizedEngine.isVectorizable(stmt) {
        // Execute with vectorized engine
        return vectorizedEngine.ExecuteQuery(ctx, tx, stmt)
    }
    
    // Fall back to traditional execution
    return standardExecutor.ExecuteQuery(ctx, tx, stmt)
}
```

## Overview

Based on the code in `/internal/sql/executor/vectorized/` and test files like `/test/vectorized_execution_test.go`, Stoolap implements a vectorized execution engine that processes data in batches rather than row by row. This approach significantly improves performance for analytical queries by leveraging modern CPU architectures and SIMD (Single Instruction, Multiple Data) capabilities.

## How Vectorized Execution Works

Instead of the traditional row-by-row processing model, Stoolap's vectorized engine:

1. Organizes data from row-based storage into columnar batches (values from the same column are stored together)
2. Processes batches of values at once (typically 1024-32K rows per batch)
3. Applies operations to entire vectors of data simultaneously
4. Uses CPU-friendly memory layouts to maximize cache efficiency

## Key Components

### Batch Processing

From `/internal/sql/executor/vectorized/batch.go`:

```go
// A Batch represents a collection of columns
type Batch struct {
    // Columns organized by data type for efficient processing
    IntColumns    [][]int64
    FloatColumns  [][]float64
    StringColumns [][]string
    BoolColumns   [][]bool
    // ...
}
```

Batches organize data by column and type, allowing for optimized processing.

### SIMD Optimization

From `/internal/sql/executor/vectorized/simd.go`:

```go
// AddFloat64SIMD adds two float vectors using SIMD-friendly patterns
func AddFloat64SIMD(a, b, result []float64) {
    // Process 8 elements at a time where possible
    n := len(a)
    i := 0
    for ; i <= n-8; i += 8 {
        result[i] = a[i] + b[i]
        result[i+1] = a[i+1] + b[i+1]
        result[i+2] = a[i+2] + b[i+2]
        result[i+3] = a[i+3] + b[i+3]
        result[i+4] = a[i+4] + b[i+4]
        result[i+5] = a[i+5] + b[i+5]
        result[i+6] = a[i+6] + b[i+6]
        result[i+7] = a[i+7] + b[i+7]
    }
    // Process remaining elements
    for ; i < n; i++ {
        result[i] = a[i] + b[i]
    }
}
```

This pattern allows the Go compiler to auto-vectorize the code, leveraging SIMD instructions when available.

### Vectorized Operators

From `/internal/sql/executor/vectorized/processor.go`:

```go
// Various operator processors for different operations
type (
    // Processes filtering operations on batches
    FilterProcessor interface {
        Process(batch *Batch) *Batch
    }
    
    // Processes projections (SELECT expressions)
    ProjectionProcessor interface {
        Process(batch *Batch) *Batch
    }
    
    // ...other processor types
)
```

These processors apply operations to entire batches at once.

## Supported Vectorized Operations

Based on the implementation and test files, Stoolap supports the following operations in vectorized mode:

### Arithmetic Operations
- Addition, subtraction, multiplication, division
- Vector-vector operations (column OP column)
- Vector-scalar operations (column OP constant)

### Comparison Operations
- Equality/inequality (=, !=, <>)
- Relational operators (>, >=, <, <=)
- Type-specific optimized comparisons

### Logical Operations
- AND, OR operations for filter conditions
- NOT operator for inverting results

### String Operations
- Basic string comparisons
- Pattern matching for LIKE operations

## Performance Benefits

According to benchmark tests:

- **Analytical queries**: 2-10x speedup compared to row-by-row execution
- **Arithmetic-heavy operations**: 3-5x performance improvement
- **Filter operations**: 2-3x faster for simple predicates
- **Memory efficiency**: Better cache utilization reduces memory stalls

From `/test/vectorized_benchmark_test.go`:

```
BenchmarkVectorizedSumInt64-8       100      15234190 ns/op    // Vectorized
BenchmarkRowBasedSumInt64-8         100      47891235 ns/op    // Row-based
```

This shows vectorized execution is approximately 3x faster for summing integer columns.

## Query Types That Benefit Most

Vectorized execution provides the greatest benefit for:

1. **Analytical queries** that process large amounts of data
2. **Filter-heavy operations** with simple conditions
3. **Arithmetic computations** on numeric columns
4. **Aggregations** over large tables
5. **Projections** with simple expressions

## Example From Test Files

From `/test/simple_vectorized_test.go`:

```go
// Test a simple vectorized addition of two columns
func TestSimpleVectorizedAddition(t *testing.T) {
    // Create sample data
    const rows = 1000
    aValues := make([]float64, rows)
    bValues := make([]float64, rows)
    for i := 0; i < rows; i++ {
        aValues[i] = float64(i)
        bValues[i] = float64(i * 2)
    }
    
    // Create batch with columns
    batch := &Batch{
        FloatColumns: [][]float64{aValues, bValues},
    }
    
    // Execute vectorized addition
    result := AddFloat64Columns(batch, 0, 1)
    
    // Verify results
    for i := 0; i < rows; i++ {
        expected := float64(i) + float64(i*2)
        if result[i] != expected {
            t.Errorf("Row %d: Expected %f, got %f", i, expected, result[i])
        }
    }
}
```

## Implementation Details

### Batch Size Considerations

Stoolap uses variable batch sizes with these characteristics:

- Default batch size: 1024 rows
- Maximum batch size: 32K rows (1024 * 32)
- Batch sizes are tuned for CPU cache efficiency
- Larger batches improve SIMD utilization but use more memory

### Memory Management

The vectorized engine uses memory pooling to reduce allocations:

- Reuses batch structures when possible
- Pre-allocates column arrays for common operations
- Minimizes garbage collection overhead during query execution

### Automatic Fallback

For operations not supported by the vectorized engine, Stoolap automatically falls back to row-based execution:

- Complex expressions may execute in row mode
- Some function calls might not be vectorized
- Certain data types may have limited vectorized support

## Best Practices

For optimal performance with Stoolap's vectorized execution:

1. **Prefer simple filters**: Use basic comparison operations where possible
2. **Process data in batches**: Load and process larger datasets at once
3. **Limit complex expressions**: Simple expressions vectorize better
4. **Consider column types**: Numeric operations benefit most from vectorization
5. **Use appropriate indexing**: Even with vectorization, indexes are still important

## Current Limitations

The vectorized execution engine has some limitations:

1. Not all SQL operations are vectorized
2. Limited support for complex string operations
3. Some functions operate in row mode even in vectorized queries
4. Window functions have limited vectorization
5. Advanced join algorithms are partially vectorized

## Future Development

Based on the code and tests, future enhancements may include:

1. More vectorized operations and functions
2. Better string operation support
3. Enhanced vectorized aggregation
4. Additional data type support
5. Direct CPU SIMD instruction usage