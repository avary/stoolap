---
title: Data Compression in Stoolap
category: Performance
order: 1
---

# Data Compression in Stoolap

Stoolap implements multiple specialized compression algorithms to reduce storage requirements while maintaining query performance. This document outlines the compression techniques used in Stoolap and how they benefit different types of data.

## Compression Architecture

Stoolap uses a column-oriented compression approach:

- Each column is compressed independently
- Compression algorithms are selected based on data type and patterns
- Decompression is performed on-demand during query execution
- Different compression strategies can be applied to different columns

## Compression Algorithms

Stoolap implements the following compression algorithms:

### Dictionary Compression

Applied to columns with repeating values, especially effective for strings with low to medium cardinality:

- Maps unique values to integer identifiers
- Stores the mapping in a dictionary
- References dictionary values instead of storing duplicate values
- Particularly effective for columns like status codes, categories, countries, etc.

### Run-Length Encoding (RLE)

Optimal for columns with sequences of repeated values:

- Stores value and count pairs for consecutive identical values
- Especially effective for boolean columns, sparse columns, or sorted data
- Also beneficial for columns with low cardinality values like enum types

### Delta Encoding

Applied to columns with gradually changing numeric values:

- Stores the differences between consecutive values instead of actual values
- Works exceptionally well for timestamps, sequential IDs, or sorted numbers
- Different variants optimize for different patterns of numeric data

### Bit-Packing

Used for integer columns with values in a limited range:

- Determines the minimum number of bits needed to represent values
- Packs multiple values into a single storage unit (words)
- Effective for small integers, boolean values, and low-cardinality data

### Time-Specific Compression

Specialized for datetime and timestamp columns:

- Decomposes timestamps into components (year, month, day, hour, etc.)
- Applies different compression strategies to each component
- Particularly effective for time-series data with regular patterns

### JSON Compression

Specialized for JSON columns:

- Structure-aware compression for JSON documents
- Shares common keys and patterns across documents
- Efficiently handles nested structures and arrays

## Automatic Algorithm Selection

Stoolap automatically selects the optimal compression algorithm for each column based on:

- Data type (numeric, string, boolean, datetime, JSON)
- Data distribution and patterns
- Column cardinality (number of unique values)
- Query access patterns

## Compression Ratios

Typical compression ratios achieved in Stoolap:

- String columns with low cardinality: 10-20x
- Numeric columns with patterns: 3-10x
- Timestamp columns: 4-8x
- Boolean or flag columns: 8-16x
- JSON documents: 2-5x

## Performance Impact

Stoolap's compression affects performance in several ways:

### Benefits

- **Reduced I/O** - Less data transferred from disk to memory
- **Better cache efficiency** - More data fits in CPU cache
- **Reduced memory usage** - More data can be processed in memory
- **Some operations can work directly on compressed data**

### Considerations

- **CPU overhead** - Compression/decompression requires CPU cycles
- **Query selectivity impact** - Highly selective queries may not benefit as much
- **Write amplification** - Writes may require more processing

## Implementation Details

Stoolap's compression is implemented with the following components:

- **compression.go** - Compression interfaces and factory
- **bitpack_compressor.go** - Bit-packing implementation
- **delta_compressor.go** - Delta encoding implementation
- **dictionary_compressor.go** - Dictionary compression implementation
- **runlength_compressor.go** - Run-length encoding implementation
- **time_compressor.go** - Specialized datetime compression
- **json_compressor.go** - JSON document compression

## Best Practices

To get the most out of Stoolap's compression:

1. **Choose appropriate column data types** - Using the most specific data type improves compression
2. **Order tables on highly compressible columns** - Improves run-length and delta encoding
3. **Use enum-like values for categorization** - Improves dictionary compression
4. **Structure JSON data consistently** - Improves JSON compression
5. **Normalize data when appropriate** - Can improve overall compression ratio

## Future Improvements

Stoolap's compression system is designed to evolve with new algorithms and optimizations:

- Adaptive compression based on workload patterns
- Hybrid compression algorithms for mixed data patterns
- Runtime adjustable compression levels
- SIMD-accelerated compression/decompression