---
title: Schema Management in Stoolap
category: SQL Commands
order: 1
---

# Schema Management in Stoolap

This document covers Stoolap's schema management capabilities, including table creation, alteration, and handling of primary keys, indexes, and data types.

## Tables and Schemas

Stoolap provides standard SQL DDL (Data Definition Language) statements for managing database schemas.

### Creating Tables

Tables can be created using the standard `CREATE TABLE` syntax:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP,
    is_active BOOLEAN
);
```

### Table Constraints

When creating tables, you can specify the following constraints:

- **PRIMARY KEY** - Define a primary key constraint on one or more columns
- **NOT NULL** - Enforce that a column cannot contain NULL values

**Note**: For uniqueness constraints, use `CREATE UNIQUE INDEX` after table creation.

### Altering Tables

Tables can be modified after creation using `ALTER TABLE` statements:

```sql
-- Add a new column
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;

-- Drop a column
ALTER TABLE users DROP COLUMN is_active;

-- Rename a table
ALTER TABLE users RENAME TO system_users;
```

## Data Types

Stoolap supports the following data types:

### Numeric Types
- **INTEGER** - Signed integer number
- **FLOAT** - Floating-point number

### String Types
- **TEXT** - Variable-length character string

### Date and Time Types
- **TIMESTAMP** - Date and time

### Boolean Type
- **BOOLEAN** - True or false value

### Special Types
- **JSON** - JSON document

## Primary Keys

Primary keys uniquely identify rows in a table:

```sql
-- Single-column primary key
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name TEXT
);

-- Composite primary key
CREATE TABLE order_items (
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    PRIMARY KEY (order_id, product_id)
);
```

## Indexes

Stoolap provides several index types for optimizing queries.

### Creating Indexes

```sql
-- Create a B-tree index
CREATE INDEX idx_user_email ON users (email);

-- Create a unique index
CREATE UNIQUE INDEX idx_unique_username ON users (username);

-- Create a multi-column index
CREATE INDEX idx_name_created ON products (name, created_at);
```

### Index Types

Stoolap supports multiple index implementations:

1. **B-tree Indexes** - General-purpose indexes for equality and range queries
2. **Bitmap Indexes** - Efficient for columns with low cardinality
3. **Columnar Indexes** - Specialized indexes for efficient filtering operations
4. **Multi-column Indexes** - For queries that filter on multiple columns

### Dropping Indexes

```sql
DROP INDEX idx_user_email;
```

## Schema Information

Stoolap provides system tables and commands to query schema information:

### SHOW Commands

```sql
-- List all tables
SHOW TABLES;

-- Show table creation statement (includes structure)
SHOW CREATE TABLE users;

-- Show indexes for a table
SHOW INDEXES FROM users;
```

## Implementation Details

Under the hood, Stoolap's schema management is implemented with the following components:

- Table metadata is stored in a structured format that tracks column definitions, constraints, and indexes
- Schema changes are performed atomically, ensuring consistency
- The parser and executor collaborate to implement DDL operations
- Indexes are created in a non-blocking way when possible

## Best Practices

- Define primary keys for all tables to ensure row uniqueness
- Create indexes on columns frequently used in WHERE clauses and join conditions
- Use appropriate data types to optimize storage and query performance
- Consider using multi-column indexes for queries that filter on multiple columns
- Avoid excessive indexing, as it can impact write performance

## Limitations

- Certain ALTER TABLE operations may require significant processing time on large tables
- Currently, online schema changes for large tables may temporarily block writes
- There are limits on the number of columns and indexes per table for performance reasons