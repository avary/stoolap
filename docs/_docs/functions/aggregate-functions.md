---
title: Aggregate Functions
category: Functions
order: 1
---

# Aggregate Functions

This document describes the aggregate functions available in Stoolap based on test files and implementation details.

## Overview

Based on test files (`/test/aggregation_test.go`, `/test/count_aggregate_test.go`, `/test/first_last_aggregate_test.go`), Stoolap supports standard SQL aggregate functions that operate on multiple rows to calculate a single result. These functions are typically used with GROUP BY clauses for summarizing data.

## Supported Aggregate Functions

### COUNT

Counts the number of rows or non-NULL values:

```sql
-- Count all rows
SELECT COUNT(*) FROM table_name;

-- Count non-NULL values in a column
SELECT COUNT(column_name) FROM table_name;

-- Count distinct values
SELECT COUNT(DISTINCT column_name) FROM table_name;
```

From `/test/count_aggregate_test.go` and `/test/count_with_index_test.go`, COUNT is optimized to use available indexes when possible.

### SUM

Calculates the sum of values in a column:

```sql
-- Sum all values in a numeric column
SELECT SUM(amount) FROM orders;

-- Sum with grouping
SELECT category, SUM(amount) FROM orders GROUP BY category;
```

Used in `/test/aggregation_test.go`.

### AVG

Calculates the average (mean) of values in a column:

```sql
-- Average of all values in a numeric column
SELECT AVG(price) FROM products;

-- Average with grouping
SELECT category, AVG(price) FROM products GROUP BY category;
```

Used in `/test/aggregation_test.go`.

### MIN

Finds the minimum value in a column:

```sql
-- Minimum value in a column
SELECT MIN(price) FROM products;

-- Minimum with grouping
SELECT category, MIN(price) FROM products GROUP BY category;
```

Used in `/test/aggregation_test.go`.

### MAX

Finds the maximum value in a column:

```sql
-- Maximum value in a column
SELECT MAX(price) FROM products;

-- Maximum with grouping
SELECT category, MAX(price) FROM products GROUP BY category;
```

Used in `/test/aggregation_test.go`.

### FIRST

Returns the first value in a group, based on the order of rows:

```sql
-- First value in a column
SELECT FIRST(name) FROM users ORDER BY created_at;

-- First value with grouping
SELECT category, FIRST(name) FROM products GROUP BY category ORDER BY price;
```

From `/test/first_last_aggregate_test.go`, FIRST depends on the order of rows in the result set.

### LAST

Returns the last value in a group, based on the order of rows:

```sql
-- Last value in a column
SELECT LAST(name) FROM users ORDER BY created_at;

-- Last value with grouping
SELECT category, LAST(name) FROM products GROUP BY category ORDER BY price;
```

From `/test/first_last_aggregate_test.go`, LAST depends on the order of rows in the result set.

## Examples from Test Files

### Basic Aggregation

From `/test/aggregation_test.go`:

```sql
-- Create a test table
CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    product TEXT,
    category TEXT,
    amount FLOAT
);

-- Insert test data
INSERT INTO sales (id, product, category, amount) VALUES
(1, 'Laptop', 'Electronics', 1200.00),
(2, 'Smartphone', 'Electronics', 800.00),
(3, 'Headphones', 'Electronics', 150.00),
(4, 'T-shirt', 'Clothing', 25.00),
(5, 'Jeans', 'Clothing', 50.00),
(6, 'Shoes', 'Clothing', 70.00);

-- Simple aggregation
SELECT 
    SUM(amount) AS total_sales,
    AVG(amount) AS avg_sale,
    MIN(amount) AS min_sale,
    MAX(amount) AS max_sale,
    COUNT(*) AS transaction_count
FROM sales;

-- Aggregation with GROUP BY
SELECT 
    category,
    SUM(amount) AS category_total,
    AVG(amount) AS category_avg,
    COUNT(*) AS category_count
FROM sales
GROUP BY category;
```

### COUNT Function Variants

From `/test/count_aggregate_test.go`:

```sql
-- Create a test table
CREATE TABLE test_count (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value INTEGER,
    category TEXT
);

-- Insert test data including NULL values
INSERT INTO test_count (id, name, value, category) VALUES
(1, 'Item 1', 10, 'A'),
(2, 'Item 2', 20, 'A'),
(3, 'Item 3', NULL, 'B'),
(4, 'Item 4', 40, 'B'),
(5, 'Item 5', 50, 'C');

-- COUNT(*) - counts all rows
SELECT COUNT(*) FROM test_count;  -- Returns 5

-- COUNT(column) - counts non-NULL values in a column
SELECT COUNT(value) FROM test_count;  -- Returns 4

-- COUNT with GROUP BY
SELECT category, COUNT(*) FROM test_count GROUP BY category;

-- COUNT DISTINCT
SELECT COUNT(DISTINCT category) FROM test_count;  -- Returns 3
```

### FIRST and LAST Functions

From `/test/first_last_aggregate_test.go`:

```sql
-- Create a test table
CREATE TABLE test_first_last (
    id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    value INTEGER,
    created_at TIMESTAMP
);

-- Insert test data
INSERT INTO test_first_last (id, name, category, value, created_at) VALUES
(1, 'Item 1', 'A', 10, '2023-01-01'),
(2, 'Item 2', 'A', 20, '2023-01-02'),
(3, 'Item 3', 'B', 30, '2023-01-03'),
(4, 'Item 4', 'B', 40, '2023-01-04'),
(5, 'Item 5', 'C', 50, '2023-01-05');

-- FIRST function (returns the first value encountered)
SELECT FIRST(name) FROM test_first_last;  -- Returns 'Item 1'

-- LAST function (returns the last value encountered)
SELECT LAST(name) FROM test_first_last;  -- Returns 'Item 5'

-- FIRST and LAST with ORDER BY (affects the result)
SELECT FIRST(name) FROM test_first_last ORDER BY value DESC;  -- Returns 'Item 5'
SELECT LAST(name) FROM test_first_last ORDER BY value DESC;  -- Returns 'Item 1'

-- FIRST and LAST with GROUP BY
SELECT category, FIRST(name), LAST(name), COUNT(*) 
FROM test_first_last 
GROUP BY category 
ORDER BY category;
```

## Using Aggregate Functions with GROUP BY

The GROUP BY clause is used with aggregate functions to group rows that have the same values:

```sql
SELECT category, COUNT(*) AS count, SUM(amount) AS total, AVG(amount) AS average
FROM sales
GROUP BY category;
```

## Using Aggregate Functions with HAVING

The HAVING clause is used to filter groups based on the result of an aggregate function:

```sql
SELECT category, COUNT(*) AS count, SUM(amount) AS total
FROM sales
GROUP BY category
HAVING COUNT(*) > 1 AND SUM(amount) > 100;
```

## NULL Handling

Based on the tests, Stoolap follows standard SQL NULL handling for aggregate functions:

- NULL values are ignored by most aggregate functions (SUM, AVG, MIN, MAX)
- COUNT(*) includes all rows, regardless of NULL values
- COUNT(column) only counts non-NULL values
- If all inputs to an aggregate function are NULL, the result is typically NULL (except for COUNT, which returns 0)

## Performance Considerations

From the implementations and test files:

- COUNT(*) is optimized to use the smallest available index when possible
- COUNT DISTINCT can be more expensive for large datasets
- Aggregations benefit from proper indexing on grouped columns
- For large tables, consider using WHERE clauses to reduce the input size before aggregation

## Implementation Details

From the test files and code inspection:

- Aggregate functions are implemented in `/internal/functions/aggregate/`
- Each function has its own implementation file (avg.go, count.go, etc.)
- Optimizations exist for common patterns like COUNT(*)