---
title: CAST Operations
category: SQL Features
order: 1
---

# CAST Operations

This document explains type casting in Stoolap based on the implementation and test files.

## Overview

Based on test files (`/test/cast_test.go`, `/test/cast_simple_test.go`, `/test/cast_evaluator_test.go`), Stoolap supports both explicit type casting using the CAST function and implicit type conversion in certain contexts. Type casting allows you to convert a value from one data type to another.

## Supported Data Types

Stoolap supports casting between these data types:

- `INTEGER` or `INT`: 64-bit signed integers
- `FLOAT`: 64-bit floating-point numbers
- `TEXT`, `STRING`, `VARCHAR`, or `CHAR`: Text strings
- `BOOLEAN` or `BOOL`: Boolean values
- `TIMESTAMP`, `DATETIME`, `DATE`, or `TIME`: Date and time values
- `JSON`: JSON data format

## Explicit CAST Syntax

To explicitly cast a value, use the CAST function:

```sql
CAST(value AS type)
```

Where:
- `value` can be a column reference, literal, or expression
- `type` is one of the supported data types

## Examples from Test Files

### Basic CAST Operations

From `/test/cast_simple_test.go`:

```sql
-- String to integer
SELECT CAST('123' AS INTEGER);                -- Returns 123

-- Integer to string
SELECT CAST(42 AS TEXT);                      -- Returns '42'

-- Float to integer (truncates)
SELECT CAST(123.45 AS INTEGER);               -- Returns 123

-- Boolean to integer
SELECT CAST(TRUE AS INTEGER);                 -- Returns 1
SELECT CAST(FALSE AS INTEGER);                -- Returns 0

-- String to boolean
SELECT CAST('true' AS BOOLEAN);               -- Returns TRUE
SELECT CAST('yes' AS BOOLEAN);                -- Returns TRUE
SELECT CAST('false' AS BOOLEAN);              -- Returns FALSE
SELECT CAST('0' AS BOOLEAN);                  -- Returns FALSE

-- Number to boolean
SELECT CAST(0 AS BOOLEAN);                    -- Returns FALSE
SELECT CAST(1 AS BOOLEAN);                    -- Returns TRUE
SELECT CAST(42 AS BOOLEAN);                   -- Returns TRUE

-- Integer to float
SELECT CAST(123 AS FLOAT);                    -- Returns 123.0

-- String to timestamp
SELECT CAST('2023-05-15 14:30:00' AS TIMESTAMP); -- Returns timestamp

-- Timestamp to string
SELECT CAST(NOW() AS TEXT);                   -- Returns formatted timestamp
```

### Using CAST in WHERE Clauses

From `/test/cast_where_clause_test.go`:

```sql
-- Create a test table with mixed data types
CREATE TABLE cast_test (
    id INTEGER PRIMARY KEY,
    text_val TEXT,
    int_val INTEGER,
    float_val FLOAT,
    bool_val BOOLEAN
);

-- Insert test data
INSERT INTO cast_test VALUES 
    (1, '100', 100, 100.5, TRUE),
    (2, '200', 200, 200.5, FALSE),
    (3, 'abc', 300, 300.5, TRUE);

-- Cast text column to integer in WHERE clause
SELECT id FROM cast_test WHERE CAST(text_val AS INTEGER) > 150;  -- Returns 2
```

### Complex CAST Operations

From `/test/cast_test.go`:

```sql
-- Nested CAST operations
SELECT CAST(CAST(123.45 AS INTEGER) AS TEXT);  -- Returns '123'

-- CAST with expressions
SELECT CAST(int_val * 2 AS TEXT) FROM cast_test;

-- CAST with NULL values
SELECT CAST(NULL AS INTEGER);                  -- Returns 0
SELECT CAST(NULL AS TEXT);                     -- Returns '' (empty string)
SELECT CAST(NULL AS BOOLEAN);                  -- Returns FALSE
```

## Type Conversion Rules

### To INTEGER

- **From FLOAT**: Truncates decimal portion (123.45 → 123)
- **From TEXT**: Parses numeric string ("123" → 123), non-numeric strings become 0
- **From BOOLEAN**: TRUE → 1, FALSE → 0
- **From TIMESTAMP**: Converts to Unix timestamp (seconds since epoch)
- **From NULL**: Returns 0

### To FLOAT

- **From INTEGER**: Direct conversion (123 → 123.0)
- **From TEXT**: Parses numeric string ("123.45" → 123.45), non-numeric strings become 0.0
- **From BOOLEAN**: TRUE → 1.0, FALSE → 0.0
- **From TIMESTAMP**: Converts to Unix timestamp with fractional seconds
- **From NULL**: Returns 0.0

### To TEXT

- **From INTEGER**: String representation (123 → "123")
- **From FLOAT**: String representation (123.45 → "123.45")
- **From BOOLEAN**: "true" or "false"
- **From TIMESTAMP**: ISO 8601 format ("2023-05-15T14:30:00Z")
- **From NULL**: Returns empty string ("")

### To BOOLEAN

- **From INTEGER**: 0 → FALSE, non-zero → TRUE
- **From FLOAT**: 0.0 → FALSE, non-zero → TRUE
- **From TEXT**: "true", "t", "yes", "y", "1" → TRUE (case-insensitive)
                "false", "f", "no", "n", "0", "" → FALSE (case-insensitive)
- **From NULL**: Returns FALSE

### To TIMESTAMP

- **From INTEGER**: Interpreted as Unix timestamp
- **From FLOAT**: Interpreted as Unix timestamp with fractional seconds
- **From TEXT**: Parses date/time string in various formats
- **From NULL**: Returns zero time value

### To JSON

- **From TEXT**: Validated as JSON string
- **From other types**: Converted to JSON representation

## Implicit Type Conversion

Stoolap performs implicit type conversion in these contexts:

1. **Arithmetic operations**: When mixing numeric types
   ```sql
   SELECT 1 + 2.5;  -- INTEGER implicitly converted to FLOAT
   ```

2. **Comparison operations**: When comparing different types
   ```sql
   SELECT * FROM table WHERE id = '100';  -- String implicitly converted to INTEGER
   ```

3. **Function arguments**: When a function expects specific types
   ```sql
   SELECT ABS('-123');  -- String implicitly converted to number
   ```

## NULL Handling in CAST Operations

As shown in the test files, casting NULL values follows these rules:

- NULL cast to INTEGER becomes 0
- NULL cast to FLOAT becomes 0.0
- NULL cast to TEXT becomes empty string ("")
- NULL cast to BOOLEAN becomes FALSE
- NULL cast to TIMESTAMP becomes zero time

## Best Practices

1. **Use explicit casts** for clarity when type conversion is intentional

2. **Be aware of data loss** when casting from higher precision to lower precision types (FLOAT to INTEGER loses decimal portion)

3. **Validate inputs** when casting from TEXT to ensure they're in the expected format

4. **Consider NULL handling** when casting columns that might contain NULL values

## Implementation Details

Stoolap implements CAST operations in various components:

- The syntax parser (`/internal/parser/cast_parser.go`) recognizes CAST expressions
- The scalar function implementation (`/internal/functions/scalar/cast.go`) handles the CAST logic
- Type-specific cast expressions (`/internal/storage/expression/cast_expression.go`) optimize CAST operations in storage layer

Vectorized execution is applied where possible to improve CAST performance for large result sets.