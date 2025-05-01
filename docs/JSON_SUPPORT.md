# JSON Data Type Support

This document describes the current state of JSON support in the Stoolap database system.

## Currently Implemented Features

The JSON data type in Stoolap has the following working features:

- **Schema Support**:
  - Can create tables with JSON columns: `CREATE TABLE example (id INTEGER, data JSON)`
  - Supports both nullable and NOT NULL constraints for JSON columns

- **Basic Operations**:
  - Can insert JSON data (objects, arrays, nested structures) as strings
  - Can insert NULL values into nullable JSON columns
  - Can retrieve JSON values (returns them as strings)
  - Can update JSON values with new JSON strings

- **Where Clauses**:
  - Can use non-JSON columns (like ID) in WHERE clauses to filter rows with JSON data
  - Basic filtering on non-JSON columns works as expected

## Implementation Details

- **Storage**: JSON data is stored as strings internally
- **Validation**: There is currently no validation of JSON syntax during insertion or updates
- **Performance**: No special indexing optimizations for JSON yet
- **Compression**: Storage includes specialized JSON compression and field name dictionary support

## Not Yet Implemented

The following JSON features are not yet implemented:

- **JSON Validation**: Invalid JSON syntax is accepted without validation
- **JSON Operations in WHERE Clauses**:
  - `IS NULL` operators not supported for JSON columns
  - JSON equality comparison in WHERE clauses not supported
  - No support for pattern matching on JSON content

- **JSON Path Expressions**:
  - No support for JSON path extraction (like `$.name` or `$.address.city`)
  - No support for accessing array elements by index (like `$.tags[0]`)

- **JSON Functions**:
  - No `JSON_EXTRACT` for path-based value extraction
  - No `JSON_OBJECT` for creating JSON objects
  - No `JSON_ARRAY` for creating JSON arrays
  - No `JSON_SET`, `JSON_INSERT`, or `JSON_REMOVE` for modifying JSON data
  - No `JSON_CONTAINS` or `JSON_CONTAINS_PATH` for checking JSON content
  - No `JSON_TYPE` for determining JSON value types

## Testing Coverage

The JSON implementation is tested with the following test files:

1. `/test/json_test.go` - Basic JSON parsing and conversion tests
2. `/test/json_simple_test.go` - Tests for JSON validation, regex matching, and storage conversion
3. `/test/json_sql_test.go` - Tests for SQL operations with JSON data
4. `/test/json_extended_test.go` - Tests for advanced JSON features (mostly skipped as features not implemented)

## Future Roadmap

Suggested extensions to JSON support:

1. Add JSON validation when inserting or updating JSON data
2. Implement basic JSON path extraction expressions
3. Add support for equality comparison and NULL checks in WHERE clauses
4. Implement common JSON functions (JSON_EXTRACT, JSON_OBJECT, etc.)
5. Add indexing support for JSON fields to improve query performance
6. Add JSON path-based update capabilities
7. Implement array operations for JSON data