# Stoolap with GORM Sample Application

This is a sample application demonstrating how to use GORM with Stoolap as the database backend.

## Features

- Connects to Stoolap using the standard database/sql driver
- Uses GORM for object-relational mapping
- Demonstrates CRUD operations with two models: User and Product
- Uses in-memory database for easy testing

## Running the Application

To run the application:

```bash
cd /path/to/stoolap/works/gormapp
go mod tidy  # Ensure all dependencies are installed
go run .
```

## Implementation Notes

- This sample uses the MySQL dialect for GORM as a generic SQL dialect
- It uses an in-memory Stoolap database (`memory://`) for demonstration
- For a persistent database, change the DSN to `file:///path/to/database`

## Stoolap Compatibility Notes

Since Stoolap has some SQL syntax differences from traditional databases, this sample application:

1. Creates tables manually instead of using GORM's AutoMigrate
2. Avoids default values in column definitions
3. Handles records one by one instead of batch operations
4. Uses simpler SQL commands that are supported by Stoolap
5. Adds the VERSION() function to Stoolap to support GORM's version checks

This approach allows GORM to work with Stoolap while respecting its current SQL dialect limitations.
