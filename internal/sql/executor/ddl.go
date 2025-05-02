package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// executeCreateIndex executes a CREATE INDEX statement
func (e *Executor) executeCreateIndex(tx storage.Transaction, stmt *parser.CreateIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Get the index name
	indexName := stmt.IndexName.Value

	// Check if the index already exists
	indexExists, err := e.engine.IndexExists(indexName, tableName)
	if err != nil {
		return err
	}
	if indexExists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("index %s already exists on table %s", indexName, tableName)
	}

	// Extract column names from the index definition
	columns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = col.Value
	}

	// Determine if this is a unique index
	isUnique := stmt.IsUnique

	// Create the index
	return tx.CreateTableIndex(tableName, indexName, columns, isUnique)
}

// executeDropIndex executes a DROP INDEX statement
func (e *Executor) executeDropIndex(tx storage.Transaction, stmt *parser.DropIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Get the index name
	indexName := stmt.IndexName.Value

	// Check if the index exists
	indexExists, err := e.engine.IndexExists(indexName, tableName)
	if err != nil {
		return err
	}
	if !indexExists {
		if stmt.IfExists {
			// If IF EXISTS is specified, don't error
			return nil
		}
		return fmt.Errorf("index %s does not exist on table %s", indexName, tableName)
	}

	// Drop the index
	return tx.DropTableIndex(tableName, indexName)
}

// executeCreateColumnarIndex executes a CREATE COLUMNAR INDEX statement
func (e *Executor) executeCreateColumnarIndex(tx storage.Transaction, stmt *parser.CreateColumnarIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Get the column name
	columnName := stmt.ColumnName.Value

	// Determine if this is a unique index
	isUnique := stmt.IsUnique

	// Check if the index already exists
	indexExists, err := e.engine.IndexExists(columnName, tableName)
	if err != nil {
		return err
	}

	if indexExists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("columnar index for column %s already exists on table %s", columnName, tableName)
	}

	// Create the columnar index
	return tx.CreateTableColumnarIndex(tableName, columnName, isUnique)
}

// executeDropColumnarIndex executes a DROP COLUMNAR INDEX statement
func (e *Executor) executeDropColumnarIndex(tx storage.Transaction, stmt *parser.DropColumnarIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Get the column name
	columnName := stmt.ColumnName.Value

	// Check if the index already exists
	indexExists, err := e.engine.IndexExists(columnName, tableName)
	if err != nil {
		return err
	}

	if !indexExists {
		if stmt.IfExists {
			// If IF EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("columnar index for column %s not exists on table %s", columnName, tableName)
	}

	// Drop the columnar index
	return tx.DropTableColumnarIndex(tableName, columnName)
}

// executeAlterTable executes an ALTER TABLE statement
func (e *Executor) executeAlterTable(tx storage.Transaction, stmt *parser.AlterTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Process the table operation based on the operation type
	switch stmt.Operation {
	case parser.AddColumn:
		// Handle ADD COLUMN operation
		if stmt.ColumnDef == nil {
			return fmt.Errorf("missing column definition for ADD COLUMN operation")
		}

		// Convert the column definition
		dataType, err := convertDataTypeFromString(stmt.ColumnDef.Type)
		if err != nil {
			return err
		}

		// Create the storage column
		// Get the table to get a proper column implementation
		table, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		// Create the column directly on the table
		nullable := true // Default to nullable
		if stmt.ColumnDef.Constraints != nil {
			for _, constraint := range stmt.ColumnDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		return table.CreateColumn(stmt.ColumnDef.Name.Value, dataType, nullable)

	case parser.DropColumn:
		// Handle DROP COLUMN operation
		if stmt.ColumnName == nil {
			return fmt.Errorf("missing column name for DROP COLUMN operation")
		}

		// Drop the column
		return tx.DropTableColumn(tableName, stmt.ColumnName.Value)

	case parser.RenameColumn:
		// Handle RENAME COLUMN operation
		if stmt.ColumnName == nil || stmt.NewColumnName == nil {
			return fmt.Errorf("missing column names for RENAME COLUMN operation")
		}

		// Rename the column
		return tx.RenameTableColumn(tableName, stmt.ColumnName.Value, stmt.NewColumnName.Value)

	case parser.ModifyColumn:
		// Handle MODIFY COLUMN operation
		if stmt.ColumnDef == nil {
			return fmt.Errorf("missing column definition for MODIFY COLUMN operation")
		}

		// Convert the column definition
		dataType, err := convertDataTypeFromString(stmt.ColumnDef.Type)
		if err != nil {
			return err
		}

		// Get the table to modify the column
		table, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		// First drop the old column
		err = table.DropColumn(stmt.ColumnDef.Name.Value)
		if err != nil {
			return err
		}

		// Create the column with new properties
		nullable := true // Default to nullable
		if stmt.ColumnDef.Constraints != nil {
			for _, constraint := range stmt.ColumnDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		// Modify the column
		return table.CreateColumn(stmt.ColumnDef.Name.Value, dataType, nullable)

	case parser.RenameTable:
		// Handle RENAME TABLE operation
		if stmt.NewTableName == nil {
			return fmt.Errorf("missing new table name for RENAME TABLE operation")
		}

		// Rename the table
		return tx.RenameTable(tableName, stmt.NewTableName.Value)

	default:
		return fmt.Errorf("unsupported ALTER TABLE operation: %v", stmt.Operation)
	}
}

// executeCreateTable executes a CREATE TABLE statement
func (e *Executor) executeCreateTable(tx storage.Transaction, stmt *parser.CreateTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table already exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if exists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("table %s already exists", tableName)
	}

	// Create a schema for the table
	schema := storage.Schema{
		TableName: tableName,
		Columns:   make([]storage.SchemaColumn, len(stmt.Columns)),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Fill in column definitions
	for i, colDef := range stmt.Columns {
		// Convert the column type
		dataType, err := convertDataTypeFromString(colDef.Type)
		if err != nil {
			return err
		}

		// Check for NOT NULL constraint
		nullable := true
		if colDef.Constraints != nil {
			for _, constraint := range colDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		// Check for PRIMARY KEY constraint
		primaryKey := false
		if colDef.Constraints != nil {
			for _, constraint := range colDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "PRIMARY KEY") {
					primaryKey = true
					break
				}
			}
		}

		// Add column to schema
		schema.Columns[i] = storage.SchemaColumn{
			ID:         i,
			Name:       colDef.Name.Value,
			Type:       dataType,
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		}
	}

	// Create the table with the schema
	_, err = tx.CreateTable(tableName, schema)
	return err
}

// executeDropTable executes a DROP TABLE statement
func (e *Executor) executeDropTable(tx storage.Transaction, stmt *parser.DropTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		if stmt.IfExists {
			// If IF EXISTS is specified, don't error
			return nil
		}
		return storage.ErrTableNotFound
	}

	// Drop the table
	return tx.DropTable(tableName)
}
