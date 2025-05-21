---
title: Scalar Functions
category: Functions
order: 1
---

# Scalar Functions

This document describes the scalar functions available in Stoolap based on the implementation found in `/internal/functions/scalar/` and corresponding test files.

## Overview

Stoolap supports a variety of scalar functions that operate on a single row and return a single value. These functions cover string manipulation, mathematical operations, date/time processing, and type conversion.

## String Functions

### UPPER
Converts a string to uppercase.

```sql
-- Syntax
UPPER(string)

-- Examples
SELECT UPPER('hello');                     -- Returns 'HELLO'
SELECT UPPER(name) FROM users;             -- Converts name column to uppercase
```

### LOWER
Converts a string to lowercase.

```sql
-- Syntax
LOWER(string)

-- Examples
SELECT LOWER('HELLO');                     -- Returns 'hello'
SELECT LOWER(email) FROM users;            -- Converts email column to lowercase
```

### LENGTH
Returns the length of a string.

```sql
-- Syntax
LENGTH(string)

-- Examples
SELECT LENGTH('hello');                    -- Returns 5
SELECT LENGTH(description) FROM products;  -- Returns length of description column
```

### SUBSTRING
Extracts a substring from a string starting at a specified position.

```sql
-- Syntax
SUBSTRING(string, start_position[, length])

-- Examples
SELECT SUBSTRING('hello world', 1, 5);     -- Returns 'hello'
SELECT SUBSTRING('hello world', 7);        -- Returns 'world'
SELECT SUBSTRING(name, 1, 3) FROM users;   -- First 3 characters of name column
```

Notes:
- Position is 1-indexed (the first character is at position 1)
- If length is omitted, returns characters from start_position to the end

### CONCAT
Concatenates two or more strings.

```sql
-- Syntax
CONCAT(string1, string2[, ...])

-- Examples
SELECT CONCAT('hello', ' ', 'world');          -- Returns 'hello world'
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;
```

### COLLATE
Applies a specific collation to a string for sorting and comparison.

```sql
-- Syntax
COLLATE(string, collation_name)

-- Examples
SELECT COLLATE('Hello', 'NOCASE');             -- For case-insensitive comparison
SELECT * FROM users ORDER BY COLLATE(name, 'NOCASE');
```

Supported collations:
- `BINARY` - Binary comparison (case-sensitive, accent-sensitive)
- `NOCASE`, `CASE_INSENSITIVE` - Case-insensitive comparison
- `NOACCENT`, `ACCENT_INSENSITIVE` - Accent-insensitive comparison
- `NUMERIC` - Compare strings as numbers where possible

## Numeric Functions

### ABS
Returns the absolute value of a number.

```sql
-- Syntax
ABS(number)

-- Examples
SELECT ABS(-10);                           -- Returns 10
SELECT ABS(balance) FROM accounts;         -- Absolute value of balance column
```

### ROUND
Rounds a number to a specified number of decimal places.

```sql
-- Syntax
ROUND(number[, decimal_places])

-- Examples
SELECT ROUND(3.14159);                     -- Returns 3 (rounds to 0 decimal places)
SELECT ROUND(3.14159, 2);                  -- Returns 3.14
SELECT ROUND(price, 2) FROM products;      -- Rounds price to 2 decimal places
```

### CEILING / CEIL
Returns the smallest integer greater than or equal to the specified number.

```sql
-- Syntax
CEILING(number)
CEIL(number)                               -- Alternative syntax

-- Examples
SELECT CEILING(3.14);                      -- Returns 4
SELECT CEILING(-3.14);                     -- Returns -3
SELECT CEILING(price) FROM products;       -- Smallest integer >= price
```

### FLOOR
Returns the largest integer less than or equal to the specified number.

```sql
-- Syntax
FLOOR(number)

-- Examples
SELECT FLOOR(3.14);                        -- Returns 3
SELECT FLOOR(-3.14);                       -- Returns -4
SELECT FLOOR(price) FROM products;         -- Largest integer <= price
```

## Date and Time Functions

### NOW
Returns the current date and time.

```sql
-- Syntax
NOW()

-- Examples
SELECT NOW();                              -- Returns current timestamp
INSERT INTO logs (event, timestamp) VALUES ('login', NOW());
```

### DATE_TRUNC
Truncates a timestamp to the specified precision.

```sql
-- Syntax
DATE_TRUNC(unit, timestamp)

-- Examples
SELECT DATE_TRUNC('day', NOW());           -- Truncates to start of current day
SELECT DATE_TRUNC('month', '2023-05-15');  -- Returns '2023-05-01'
SELECT DATE_TRUNC('year', created_at) FROM orders;
```

Supported units:
- `year` - Truncates to the beginning of the year
- `month` - Truncates to the beginning of the month
- `day` - Truncates to the beginning of the day
- `hour` - Truncates to the beginning of the hour
- `minute` - Truncates to the beginning of the minute
- `second` - Truncates to the beginning of the second

### TIME_TRUNC
Truncates a timestamp to a specified time interval.

```sql
-- Syntax
TIME_TRUNC(interval, timestamp)

-- Examples
SELECT TIME_TRUNC('15m', NOW());           -- Truncates to 15-minute interval
SELECT TIME_TRUNC('1h', '2023-05-15 14:27:36');  -- Returns '2023-05-15 14:00:00'
SELECT TIME_TRUNC('30m', timestamp) FROM events;
```

Supported intervals:
- Time units with a multiplier: '15m', '30m', '1h', '4h', etc.
- 'm' (minutes), 'h' (hours), 'd' (days)

## Conversion Functions

### CAST
Converts a value from one data type to another.

```sql
-- Syntax
CAST(value AS type)

-- Examples
SELECT CAST('123' AS INTEGER);             -- Returns 123
SELECT CAST(1 AS TEXT);                    -- Returns '1'
SELECT CAST(price AS INTEGER) FROM products;
```

Supported type conversions:
- `INTEGER` or `INT`: Convert to integer
- `FLOAT`, `REAL`, or `DOUBLE`: Convert to floating-point
- `TEXT`, `STRING`, `VARCHAR`, or `CHAR`: Convert to string
- `BOOLEAN` or `BOOL`: Convert to boolean
- `TIMESTAMP`, `DATETIME`, `DATE`, or `TIME`: Convert to timestamp
- `JSON`: Convert to JSON

### COALESCE
Returns the first non-NULL value in a list of expressions.

```sql
-- Syntax
COALESCE(value1, value2[, ...])

-- Examples
SELECT COALESCE(NULL, 'default');          -- Returns 'default'
SELECT COALESCE(middle_name, '', last_name) FROM users;
```

## Example Queries Using Multiple Functions

### User Name Formatting
```sql
SELECT 
    id,
    COALESCE(title, '') || ' ' || 
    UPPER(SUBSTRING(first_name, 1, 1)) || '. ' || 
    last_name AS formatted_name
FROM users;
```

### Price Formatting
```sql
SELECT 
    product_name,
    CONCAT('$', CAST(ROUND(price, 2) AS TEXT)) AS formatted_price
FROM products;
```

### Date Grouping
```sql
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

## Performance Considerations

- Scalar functions are evaluated for each row in the result set
- Some functions benefit from vectorized execution for better performance
- Using functions in WHERE clauses may prevent the use of indexes
- Complex expressions with multiple functions can impact query performance

## Implementation Details

- Stoolap scalar functions are implemented in Go in the `/internal/functions/scalar/` directory
- Each function is optimized for type-specific operations
- NULL handling follows SQL standards (most functions return NULL if any input is NULL)
- Functions support Stoolap's vectorized execution engine where applicable