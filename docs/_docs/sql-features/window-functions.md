---
title: Window Functions
category: SQL Features
order: 1
---

# Window Functions

This document explains window functions in Stoolap, including current implementation status, syntax, and examples.

## Overview

Window functions perform calculations across a set of table rows that are related to the current row. Unlike regular aggregate functions, window functions do not cause rows to become grouped into a single output row — the rows retain their separate identities while allowing you to perform calculations using other rows.

## Current Implementation Status

Based on the test files (`/test/window_function_test.go`), Stoolap currently has a very basic implementation of window functions with **limited functionality**. The current ROW_NUMBER() implementation is minimal and, as shown in the test files, returns a fixed value of 1 for all rows.

### Currently Available

- **ROW_NUMBER()**: A very basic implementation that returns the value 1 for all rows.

The ROW_NUMBER() function is registered and can be used in queries, but the implementation is not yet complete to provide actual row numbering functionality.

## Basic Syntax

The syntax for using window functions in Stoolap is:

```sql
window_function_name() OVER ()
```

## Basic Usage Example

```sql
-- Basic window function example
SELECT id, name, department, ROW_NUMBER() OVER () as row_num
FROM employees;
```

Note that in the current implementation, this would assign the value 1 to all rows.

## Example Based on Test Case

The following example is taken directly from the test file:

```sql
-- Test table structure
CREATE TABLE window_test (
    id INTEGER,
    name TEXT,
    department TEXT,
    salary INTEGER,
    hire_date TIMESTAMP
);

-- Sample data
INSERT INTO window_test (id, name, department, salary, hire_date) VALUES 
(1, 'Alice', 'Engineering', 85000, '2021-03-15'),
(2, 'Bob', 'Engineering', 75000, '2022-01-10'),
(3, 'Charlie', 'Engineering', 90000, '2020-11-05'),
(4, 'Diana', 'Marketing', 65000, '2021-08-22'),
(5, 'Eve', 'Marketing', 70000, '2022-05-17'),
(6, 'Frank', 'Finance', 95000, '2020-05-01'),
(7, 'Grace', 'Finance', 85000, '2021-10-12');

-- Query with ROW_NUMBER()
SELECT ROW_NUMBER() AS row_num
FROM window_test
ORDER BY id;
```

In the current implementation, this query would return 1 for each row, resulting in 7 rows each with row_num = 1.

## Future Development

Full window function support is planned for future releases of Stoolap. This will include:

- Complete implementation of ROW_NUMBER() to return actual row numbers
- Support for PARTITION BY and ORDER BY clauses
- Additional window functions such as RANK(), DENSE_RANK(), etc.
- Support for window frames
- Aggregation functions over windows

## Limitations of Current Implementation

The current implementation has several limitations:

1. ROW_NUMBER() always returns 1, regardless of ordering
2. No support for PARTITION BY clauses
3. No support for ORDER BY clauses within the OVER() specification
4. No other window functions are implemented

## Best Practices

Given the current implementation limitations:

1. ROW_NUMBER() can be used, but be aware that it will return 1 for all rows
2. For analytical needs, consider using GROUP BY with aggregate functions instead
3. Check release notes for updates on window function implementation