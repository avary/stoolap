---
title: JSON Support
category: Data Types
order: 1
---

# JSON Support

This document details Stoolap's current JSON data type support, capabilities, and best practices for working with JSON data based on the implemented test cases.

## Introduction to JSON in Stoolap

Based on test files (`/test/json_simple_test.go`, `/test/json_extended_test.go`, `/test/json_sql_test.go`), Stoolap provides native support for JSON (JavaScript Object Notation) data, allowing you to store structured data alongside your conventional relational data. The current implementation focuses on:

- Basic JSON storage and validation
- Support for JSON data types in tables
- Equality comparison for JSON values

## JSON Data Type

Stoolap implements a dedicated JSON data type:

```sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  attributes JSON
);
```

The JSON data type in Stoolap supports:

- **Objects** - Collection of key-value pairs: `{"name": "value", "name2": "value2"}`
- **Arrays** - Ordered collection of values: `[1, 2, 3, "text", true]`
- **Nested structures** - Complex combinations of objects and arrays
- **Primitive values** - Numbers, strings, booleans, and null
- **NULL constraints** - `NOT NULL` constraints can be applied to JSON columns

## Basic JSON Operations

### Inserting JSON Data

```sql
-- Insert as a JSON string
INSERT INTO products (id, name, attributes)
VALUES (1, 'Smartphone', '{"brand": "Example", "color": "black", "specs": {"ram": 8, "storage": 128}}');

-- Insert null into nullable JSON column
INSERT INTO products (id, name, attributes)
VALUES (2, 'Headphones', NULL);

-- Using parameter binding with JSON
INSERT INTO products (id, name, attributes) VALUES (?, ?, ?);
-- With values: 3, 'Tablet', '{"brand":"Example","model":"T500"}'
```

### Retrieving JSON Data

```sql
-- Fetch entire JSON values
SELECT id, name, attributes FROM products;

-- Filter by non-JSON columns
SELECT id, attributes FROM products WHERE name = 'Smartphone';
```

### Updating JSON Data

```sql
-- Update entire JSON value
UPDATE products 
SET attributes = '{"brand": "Example", "color": "red", "specs": {"ram": 16, "storage": 256}}'
WHERE id = 1;
```

## JSON Validation

As shown in the test files, Stoolap validates JSON syntax during insertion:

```sql
-- Valid JSON will be accepted
INSERT INTO products (id, name, attributes) VALUES (4, 'Valid', '{"brand":"Example"}');

-- Invalid JSON will be rejected
INSERT INTO products (id, name, attributes) VALUES (5, 'Invalid', '{brand:"Example"}');
-- Error: Invalid JSON format
```

The test files (`/test/json_simple_test.go`) validate these examples of properly formatted JSON:

```
{"name":"John","age":30}
[1,2,3,4]
{"user":{"name":"John","age":30}}
[{"name":"John"},{"name":"Jane"}]
[]
{}
{"":""}
```

And these examples of invalid JSON:

```
{name:"John"}        -- Missing quotes around property name
{"name":"John"       -- Missing closing brace
{"name":"John",}     -- Trailing comma
{"name":John}        -- Missing quotes around string value
{name}               -- Invalid format
[1,2,3,}             -- Mismatched brackets
```

## Application Integration

When using Go with Stoolap, you can work with JSON data using standard libraries as shown in the test files:

```go
// Insert JSON from a Go struct/map
type Product struct {
    Brand  string `json:"brand"`
    Color  string `json:"color"`
}

product := Product{Brand: "Example", Color: "blue"}
productJSON, _ := json.Marshal(product)
_, err = db.Exec("INSERT INTO products (id, name, attributes) VALUES (?, ?, ?)", 
                 6, "Widget", string(productJSON))

// Query and parse JSON data
var attributes string
err = db.QueryRow("SELECT attributes FROM products WHERE id = 6").Scan(&attributes)

var parsedProduct Product
json.Unmarshal([]byte(attributes), &parsedProduct)
```

## Current Limitations

Based on the skipped tests in `/test/json_extended_test.go`, the current JSON implementation has some limitations:

- No JSON path extraction (e.g., no `$.property` syntax)
- No JSON modification functions (JSON_SET, JSON_INSERT, etc.)
- No JSON construction functions (JSON_OBJECT, JSON_ARRAY, etc.)
- Basic equality comparison only
- No indexing of JSON properties

As explicitly noted in the test files, these features are skipped and marked as "not implemented" in the test assertions.

## Test-Based Example

From `/test/json_extended_test.go`, here's a real example showcasing JSON functionality:

```sql
-- Create a table with JSON column
CREATE TABLE json_extended (
    id INTEGER NOT NULL,
    data JSON
);

-- Insert test data
INSERT INTO json_extended (id, data) VALUES 
(1, '{"name":"John","age":30,"address":{"city":"New York","zip":"10001"},"tags":["developer","manager"]}'),
(2, '{"name":"Alice","age":25,"address":{"city":"Boston","zip":"02108"},"tags":["designer","artist"]}'),
(3, '{"name":"Bob","age":null,"address":null,"tags":[]}'),
(4, '[1,2,3,4,5]'),
(5, '{"numbers":[1,2,3,4,5],"nested":{"a":1,"b":2}}');

-- Simple equality comparison (supported)
SELECT id FROM json_extended WHERE data = '{"name":"John","age":30,"address":{"city":"New York","zip":"10001"},"tags":["developer","manager"]}';
```

## Best Practices

Based on the limitations identified in the test files:

### Schema Design

- **Hybrid approach**: Store frequently queried fields in regular columns, use JSON for flexible/nested data
- **Don't overuse**: Don't use JSON to avoid proper data modeling
- **Keep it simple**: Since advanced JSON operations aren't yet supported, use simple JSON structures

### Implementation Tips

- **Validate JSON**: Always validate JSON in your application before insertion
- **Size management**: Keep JSON documents reasonably sized
- **Type safety**: Use proper typing when working with JSON in your application code

## Future JSON Features

The following features are skipped in the tests with comments indicating they may be implemented in future releases:

- JSON path extraction (similar to `JSON_EXTRACT()`) marked as "JSON path extraction not implemented"
- JSON modification functions (JSON_SET, JSON_INSERT, etc.) marked as "JSON_SET function not implemented"
- JSON construction functions (JSON_OBJECT, JSON_ARRAY, etc.) marked as "JSON_OBJECT function not implemented"
- JSON comparison functions (JSON_CONTAINS, etc.) marked as "JSON_CONTAINS function not implemented"