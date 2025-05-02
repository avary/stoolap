# Data Types in Stoolap

Stoolap supports a variety of data types to handle different kinds of information in your database. Below is a detailed explanation of each data type, including usage examples and format specifications.

## Supported Data Types

### INTEGER

The INTEGER data type stores whole numbers without a fractional component.

- **Storage**: Stored as signed 64-bit integers (int64)
- **Range**: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- **Usage Example**:
  ```sql
  CREATE TABLE products (
      id INTEGER PRIMARY KEY,
      quantity INTEGER NOT NULL
  );
  
  INSERT INTO products (id, quantity) VALUES (1, 100);
  ```

### FLOAT

The FLOAT data type stores approximate numeric data with a decimal point.

- **Storage**: Stored as 64-bit floating point numbers (float64)
- **Range**: Approximately ±1.8E308 with 15 digits of precision
- **Usage Example**:
  ```sql
  CREATE TABLE measurements (
      id INTEGER PRIMARY KEY,
      temperature FLOAT,
      weight FLOAT
  );
  
  INSERT INTO measurements (id, temperature, weight) VALUES (1, 98.6, 72.5);
  ```

### TEXT

The TEXT data type stores character strings of variable length.

- **Storage**: Stored as UTF-8 strings
- **Usage Example**:
  ```sql
  CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT
  );
  
  INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');
  ```

### BOOLEAN

The BOOLEAN data type stores truth values: true or false.

- **Storage**: Stored as a boolean value
- **Values**: TRUE, FALSE, or NULL
- **Usage Example**:
  ```sql
  CREATE TABLE user_settings (
      user_id INTEGER PRIMARY KEY,
      notifications_enabled BOOLEAN DEFAULT TRUE,
      dark_mode BOOLEAN DEFAULT FALSE
  );
  
  INSERT INTO user_settings (user_id, notifications_enabled, dark_mode) VALUES (1, TRUE, FALSE);
  ```

### TIMESTAMP

The TIMESTAMP data type stores a point in time (date and time).

- **Storage**: Stored as a Go time.Time value
- **Format**: Many formats supported, but RFC3339 is recommended: 'YYYY-MM-DDTHH:MM:SSZ'
- **Usage Example**:
  ```sql
  CREATE TABLE events (
      id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      created_at TIMESTAMP
  );
  
  INSERT INTO events (id, title, created_at) VALUES (1, 'Conference', '2023-05-15T14:30:00Z');
  ```

### JSON

The JSON data type stores JSON (JavaScript Object Notation) formatted data.

- **Storage**: Can be stored as a string or as a parsed Go map/slice structure
- **Format**: Valid JSON object or array format
- **Usage Example**:
  ```sql
  CREATE TABLE user_profiles (
      user_id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      metadata JSON
  );
  
  INSERT INTO user_profiles (user_id, name, metadata) VALUES (
      1, 
      'John Doe', 
      '{"department":"Engineering","skills":["Go","SQL"],"address":{"city":"New York","zip":"10001"}}'
  );
  ```

## Type Conversion

Stoolap provides automatic type conversion in many cases to make working with different data types more convenient:

- **Numeric Conversion**: INTEGER values can be converted to FLOAT and vice versa (with potential precision loss)
- **String to Date/Time**: Text strings in the correct format will be automatically converted to DATE, TIME, or TIMESTAMP values
- **String to JSON**: Text strings in valid JSON format will be automatically converted to JSON values
- **Boolean Representation**: Strings like "true", "false", "1", "0" can be used for BOOLEAN values

## NULL Values

All data types in Stoolap can accept NULL values unless the column is defined with the NOT NULL constraint.

```sql
CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    required_text TEXT NOT NULL,
    optional_number INTEGER
);

INSERT INTO example (id, required_text) VALUES (1, 'This is required'); -- optional_number is NULL
```

## Default Values

Default values can be specified for columns:

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price FLOAT DEFAULT 0.0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Best Practices

1. **Use the Appropriate Type**: Choose the most appropriate data type for your data to optimize storage and performance.
2. **Date/Time Formats**: Use standard formats (ISO 8601) for date and time values.
3. **JSON Validation**: While Stoolap will accept any string as JSON, validate your JSON data before inserting it.
4. **NULL Handling**: Use NOT NULL constraints for columns that should never contain NULL values.
5. **Large Text/JSON**: For very large TEXT or JSON values, consider if they could be stored in separate tables or files.