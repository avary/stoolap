package mvcc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

var (
	ErrTransactionClosed = errors.New("transaction already closed")

	// Global pool for tables map to reduce allocations
	tablesMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]*MVCCTable)
		},
	}
)

// MVCCTransaction is an in-memory implementation of a transaction
type MVCCTransaction struct {
	id        int64
	startTime time.Time
	engine    *MVCCEngine
	tables    map[string]*MVCCTable
	active    bool
	mu        sync.RWMutex
	ctx       context.Context // Context for the transaction

	// Fast path for single table operations
	lastTableName string
	lastTable     storage.Table
}

// ID returns the transaction ID
func (t *MVCCTransaction) ID() int64 {
	return t.id
}

// SetContext sets the context for the transaction
func (t *MVCCTransaction) SetContext(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ctx = ctx
}

// Begin starts the transaction
// This is now a no-op for compatibility, as the transaction is fully initialized by NewMVCCTransaction or BeginTx
func (t *MVCCTransaction) Begin() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// The transaction ID and registry are now set during creation
	return nil
}

// Commit commits the transaction
func (t *MVCCTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	defer t.cleanUp()

	// Mark transaction as committed in registry
	t.engine.registry.CommitTransaction(t.id)

	// First check if this is a read-only transaction BEFORE committing tables
	// since committing tables will clear their local versions
	isReadOnly := true

	// Check if any tables have modifications
	for _, table := range t.tables {
		if table.txnVersions != nil && len(table.txnVersions.localVersions) > 0 {
			isReadOnly = false
			break
		}
	}

	// Now commit all MVCC tables to merge their changes to the global version stores
	for _, table := range t.tables {
		if err := table.Commit(); err != nil {
			return err
		}
	}

	// Record in WAL if persistence is enabled AND transaction modified data
	if t.engine.persistence != nil && t.engine.persistence.IsEnabled() {
		// Use the isReadOnly flag we computed BEFORE committing tables
		// At this point, most table.txnVersions will be nil because Commit() cleared them
		if !isReadOnly {
			// Collect auto-increment values from tables that have been modified in this transaction
			var autoIncrementInfo []struct {
				TableName string
				Value     int64
			}

			// Check for any version stores that may have had their auto-increment counters updated
			for tableName, vs := range t.engine.versionStores {
				autoIncValue := vs.GetCurrentAutoIncrementValue()
				if autoIncValue > 0 {
					autoIncrementInfo = append(autoIncrementInfo, struct {
						TableName string
						Value     int64
					}{
						TableName: tableName,
						Value:     autoIncValue,
					})
				}
			}

			// Record commit with auto-increment information
			err := t.engine.persistence.RecordCommit(t.id, autoIncrementInfo...)
			if err != nil {
				fmt.Printf("Warning: Failed to record commit in WAL: %v\n", err)
			}
		}
	}

	t.active = false

	return nil
}

// Rollback rolls back the transaction
func (t *MVCCTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	defer t.cleanUp()

	// Mark transaction as rolled back in registry
	t.engine.registry.AbortTransaction(t.id)

	// First check if this is a read-only transaction BEFORE rolling back tables
	// since rollback will clear the local versions
	isReadOnly := true

	// Check if any tables have modifications
	for _, table := range t.tables {
		if table.txnVersions != nil && len(table.txnVersions.localVersions) > 0 {
			isReadOnly = false
			break
		}
	}

	// Rollback all tables
	for _, table := range t.tables {
		table.Rollback()
	}

	// Record in WAL if persistence is enabled AND transaction modified data
	if t.engine.persistence != nil && t.engine.persistence.IsEnabled() {
		// Use the isReadOnly flag we computed BEFORE committing tables
		// At this point, most table.txnVersions will be nil because Commit() cleared them
		if !isReadOnly {
			err := t.engine.persistence.RecordRollback(t.id)
			if err != nil {
				fmt.Printf("Warning: Failed to record rollback in WAL: %v\n", err)
			}
		}
	}

	t.active = false

	return nil
}

// cleanUp cleans up the transaction resources
func (t *MVCCTransaction) cleanUp() error {
	if t.active {
		return errors.New("transaction is still active")
	}

	// Clear fast path cache
	t.lastTableName = ""
	t.lastTable = nil

	// Return the tables to the pool
	engine := t.engine
	for _, table := range t.tables {
		engine.ReturnTableToPool(table)
	}

	// Clear and return the tables map to the pool
	// Save a reference first since we'll set t.tables to nil
	tablesMap := t.tables
	t.tables = nil
	clear(tablesMap)
	tablesMapPool.Put(tablesMap)

	return nil
}

// AddTableColumn adds a column to a table
func (t *MVCCTransaction) AddTableColumn(tableName string, column storage.SchemaColumn) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Add the column to the table
	return table.CreateColumn(column.Name, column.Type, column.Nullable)
}

// DropTableColumn drops a column from a table
func (t *MVCCTransaction) DropTableColumn(tableName string, columnName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Drop the column from the table
	return table.DropColumn(columnName)
}

// RenameTableColumn renames a column in a table
func (t *MVCCTransaction) RenameTableColumn(tableName string, oldName, newName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Create a basic implementation for testing purposes
	tableSchema := table.Schema()

	// Check if oldName exists and newName doesn't exist
	oldColIdx := -1
	for i, col := range tableSchema.Columns {
		if col.Name == oldName {
			oldColIdx = i
		} else if col.Name == newName {
			return errors.New("column with new name already exists")
		}
	}

	if oldColIdx == -1 {
		return errors.New("column with old name not found")
	}

	// For a real implementation, we'd need more sophisticated column rename handling
	// This is a minimal implementation that modifies the schema and relies on the underlying
	// storage implementations to handle the rename

	// First create a copy of the column with the new name
	oldCol := tableSchema.Columns[oldColIdx]

	// Update the schema
	// First create a copy of the old column to preserve its values
	if err := table.CreateColumn(newName, oldCol.Type, oldCol.Nullable); err != nil {
		return err
	}

	// Drop the old column
	return table.DropColumn(oldName)
}

// ModifyTableColumn modifies a column in a table
func (t *MVCCTransaction) ModifyTableColumn(tableName string, column storage.SchemaColumn) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Create a basic implementation for testing purposes
	tableSchema := table.Schema()

	// Check if the column exists
	colName := column.Name
	colExists := false

	for _, col := range tableSchema.Columns {
		if col.Name == colName {
			colExists = true
			break
		}
	}

	if !colExists {
		return errors.New("column not found")
	}

	// For a real implementation, we'd need more sophisticated column modification handling
	// This is a simplified approach that:
	// 1. Creates a new column with the modified type
	// 2. Drops the old column

	// Create a new column with the new type
	if err := table.CreateColumn(colName+"_temp", column.Type, column.Nullable); err != nil {
		return err
	}

	// In a real implementation, we would copy the data from the old column to the new one,
	// possibly with type conversion. For this test implementation, we skip that step.

	// Drop the old column
	if err := table.DropColumn(colName); err != nil {
		return err
	}

	// Create the new column with the correct name
	if err := table.CreateColumn(colName, column.Type, column.Nullable); err != nil {
		return err
	}

	// Drop the temporary column
	return table.DropColumn(colName + "_temp")
}

// RenameTable renames a table
func (t *MVCCTransaction) RenameTable(oldName, newName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	//TODO: Implement renaming of tables in the engine
	return nil
}

// Select performs a selection query on a table
func (t *MVCCTransaction) Select(tableName string, columnsToFetch []string, where *storage.Condition, originalColumns ...string) (storage.Result, error) {
	// We don't acquire locks here since GetTable handles its own locking

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table schema
	schema := table.Schema()

	// Convert the condition to an Expression with schema awareness
	var expr storage.Expression
	if where != nil {
		// Convert the Condition to a SchemaAwareExpression
		expr = t.conditionToSchemaAwareExpression(where, table)
	}

	// Find the column indices in the schema
	var columnIndices []int
	if len(columnsToFetch) > 0 {
		columnIndices = make([]int, 0, len(columnsToFetch))

		// Map column names to indices
		colMap := make(map[string]int)
		for i, col := range schema.Columns {
			colMap[col.Name] = i
		}

		// Look up each requested column
		for _, colName := range columnsToFetch {
			if idx, ok := colMap[colName]; ok {
				columnIndices = append(columnIndices, idx)
			} else {
				return nil, fmt.Errorf("column not found: %s", colName)
			}
		}
	}

	// Scan the table with the column indices and expression
	scanner, err := table.Scan(columnIndices, expr)
	if err != nil {
		return nil, err
	}

	// Return a result that wraps the scanner
	return NewTableResult(t.ctx, scanner, schema, columnIndices, originalColumns...), nil
}

// SelectWithAliases performs a selection query with column aliases
func (t *MVCCTransaction) SelectWithAliases(tableName string, columnsToFetch []string, where *storage.Condition, aliases map[string]string, originalColumns ...string) (storage.Result, error) {
	// We don't acquire locks here since GetTable handles its own locking

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table schema
	schema := table.Schema()

	// Convert the condition to an Expression with alias handling and schema awareness
	var expr storage.Expression
	if where != nil {
		// Check if the condition has a complex expression attached
		if where.Expression != nil {
			// Use the expression directly, with aliases applied
			if len(aliases) > 0 {
				expr = where.Expression.WithAliases(aliases)
			} else {
				expr = where.Expression
			}
		} else {
			// Apply aliases to the condition
			aliasedCondition := where.WithAliases(aliases)

			// Convert the Condition to a SchemaAwareExpression
			expr = t.conditionToSchemaAwareExpression(aliasedCondition, table)
		}
	}

	// Find the column indices in the schema, resolving aliases
	var columnIndices []int
	if len(columnsToFetch) > 0 {
		columnIndices = make([]int, 0, len(columnsToFetch))

		// Map column names to indices
		colMap := make(map[string]int)
		for i, col := range schema.Columns {
			colMap[col.Name] = i
		}

		// Look up each requested column
		for _, colName := range columnsToFetch {
			// If colName is an alias, use the original column name
			actualColName := colName
			if origCol, isAlias := aliases[colName]; isAlias {
				actualColName = origCol
			}

			if idx, ok := colMap[actualColName]; ok {
				columnIndices = append(columnIndices, idx)
			} else {
				return nil, fmt.Errorf("column not found: %s", colName)
			}
		}
	}

	// Scan the table with the column indices and expression
	scanner, err := table.Scan(columnIndices, expr)
	if err != nil {
		return nil, err
	}

	// Create a base result
	baseResult := NewTableResult(t.ctx, scanner, schema, columnIndices, originalColumns...)

	// Wrap the result with alias handling
	return NewAliasedResult(baseResult, aliases), nil
}

// SelectWithExpression executes a SELECT query with a complex expression filter
// This allows pushing complex expressions (including AND/OR combinations) down to the storage layer
func (t *MVCCTransaction) SelectWithExpression(tableName string, columnsToFetch []string, filter storage.Expression, aliases map[string]string, originalColumns ...string) (storage.Result, error) {
	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table schema
	schema := table.Schema()

	// Apply aliases to the expression if needed
	var expr storage.Expression
	if filter != nil && aliases != nil && len(aliases) > 0 {
		expr = filter.WithAliases(aliases)
	} else {
		expr = filter
	}

	// Find the column indices in the schema, resolving aliases
	var columnIndices []int
	if len(columnsToFetch) > 0 {
		columnIndices = make([]int, 0, len(columnsToFetch))

		// Map column names to indices
		colMap := make(map[string]int)
		for i, col := range schema.Columns {
			colMap[col.Name] = i
		}

		// Look up each requested column
		for _, colName := range columnsToFetch {
			// If colName is an alias, use the original column name
			actualColName := colName
			if origCol, isAlias := aliases[colName]; isAlias {
				actualColName = origCol
			}

			if idx, ok := colMap[actualColName]; ok {
				columnIndices = append(columnIndices, idx)
			} else {
				return nil, fmt.Errorf("column not found: %s", colName)
			}
		}
	}

	// Scan the table with the column indices and expression
	scanner, err := table.Scan(columnIndices, expr)
	if err != nil {
		return nil, err
	}

	// Create a base result
	baseResult := NewTableResult(t.ctx, scanner, schema, columnIndices, originalColumns...)

	// Wrap the result with alias handling
	return NewAliasedResult(baseResult, aliases), nil
}

// GetFilterCapabilities returns capabilities information about what types of filters
// the storage engine can handle directly
func (t *MVCCTransaction) GetFilterCapabilities() storage.FilterCapabilities {
	return storage.FilterCapabilities{
		SupportsComplexExpressions:  true,
		SupportsFunctionExpressions: true,
		SupportedOperators: []storage.Operator{
			storage.EQ, storage.NE, storage.GT, storage.GTE, storage.LT, storage.LTE,
			storage.LIKE, storage.IN, storage.NOTIN, storage.ISNULL, storage.ISNOTNULL,
		},
		SupportsBetween:                true,
		SupportsIn:                     true,
		SupportsJSONPath:               true,
		SupportsMultiColumnExpressions: true,
		SupportsNullChecks:             true,
	}
}

// conditionToExpression converts a Condition to an Expression
func (t *MVCCTransaction) conditionToExpression(cond *storage.Condition) storage.Expression {
	var expr storage.Expression

	// Special handling for IS NULL and IS NOT NULL
	if cond.Operator == storage.ISNULL || cond.Operator == storage.ISNOTNULL {
		// For IS NULL/IS NOT NULL, we should use a NullCheckExpression
		if cond.Operator == storage.ISNULL {
			expr = expression.NewIsNullExpression(cond.ColumnName)
		} else {
			expr = expression.NewIsNotNullExpression(cond.ColumnName)
		}
	} else {
		// For other operators, use SimpleExpression
		expr = &expression.SimpleExpression{
			Column:   cond.ColumnName,
			Operator: cond.Operator,
			Value:    cond.Value,
		}
	}

	// If the condition had aliases applied, preserve that information
	if aliases := cond.GetAliases(); aliases != nil {
		// Check if we need to preserve original name for simple expressions
		if originalName := cond.GetOriginalName(); originalName != "" {
			if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
				// Store the original alias name so we can reference it later
				simpleExpr.SetOriginalColumn(originalName)
			}
		}
		// Apply aliases to the expression
		expr = expr.WithAliases(aliases)
	}

	return expr
}

// conditionToSchemaAwareExpression converts a Condition to a SchemaAwareExpression
// using the given table's schema for column name resolution
func (t *MVCCTransaction) conditionToSchemaAwareExpression(cond *storage.Condition, table storage.Table) storage.Expression {
	// First convert to regular expression
	expr := t.conditionToExpression(cond)

	// Then apply schema awareness if possible
	if table != nil {
		schema := table.Schema()
		if len(schema.Columns) > 0 {
			// Create a column name -> index mapping (single allocation)
			columnMap := make(map[string]int, len(schema.Columns))
			for i, col := range schema.Columns {
				columnMap[col.Name] = i
				// Also add lowercase version for case-insensitive lookups
				lowerName := strings.ToLower(col.Name)
				if _, exists := columnMap[lowerName]; !exists {
					columnMap[lowerName] = i
				}
			}

			// Wrap the expression with schema awareness
			if schemaExpr, ok := expr.(storage.SchemaAwareExpression); ok {
				// If the expression already supports schema awareness, use it
				expr = schemaExpr.WithSchema(columnMap)
			} else {
				// Otherwise, wrap it with a SchemaAwareExpression
				saExpr := expression.NewSchemaAwareExpression(expr, schema)
				expr = saExpr
				// Note: This is a less frequently called path, so the object leakage
				// will be minimal compared to the Delete/Update methods
			}
		}
	}

	return expr
}

// CreateTable creates a new table
func (t *MVCCTransaction) CreateTable(name string, schema storage.Schema) (storage.Table, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Check if table already exists
	if _, exists := t.tables[name]; exists {
		return nil, storage.ErrTableAlreadyExists
	}

	// Check if table exists in engine
	exists, err := t.engine.TableExists(name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, storage.ErrTableAlreadyExists
	}

	_, err = t.engine.CreateTable(schema)
	if err != nil {
		return nil, err
	}

	// Get the version store for this table
	versionStore, err := t.engine.GetVersionStore(name)
	if err != nil {
		return nil, err
	}

	// Create a table from the engine's pool
	mvccTable := t.engine.GetTableFromPool(t.id, versionStore)

	// Store table in transaction
	t.tables[name] = mvccTable

	return mvccTable, nil
}

// DropTable drops a table
func (t *MVCCTransaction) DropTable(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// Check if table exists in transaction
	if table, exists := t.tables[name]; exists {
		// Return the table to the pool
		t.engine.ReturnTableToPool(table)
		delete(t.tables, name)
	}

	// Drop the table in the engine
	return t.engine.DropTable(name)
}

// GetTable returns a table wrapper with optimized memory usage
func (t *MVCCTransaction) GetTable(name string) (storage.Table, error) {
	// Quick early check if transaction is closed before acquiring any locks
	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Fast path for repeated access to the same table (common in batch operations)
	if name == t.lastTableName && t.lastTable != nil {
		return t.lastTable, nil
	}

	// Try with a read lock to see if the table exists in the transaction's tables map
	t.mu.RLock()
	if table, exists := t.tables[name]; exists {
		// Found in the transaction's tables map, no need to cache again
		t.mu.RUnlock()

		// Update fast path cache
		t.lastTableName = name
		t.lastTable = table

		return table, nil
	}
	t.mu.RUnlock()

	// If we get here, the table doesn't exist in our transaction's tables map.
	// We need to acquire a write lock to possibly modify the tables map.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check again after getting the write lock as it might have been added
	if table, exists := t.tables[name]; exists {
		// Update fast path cache
		t.lastTableName = name
		t.lastTable = table
		return table, nil
	}

	// Check if table exists in engine
	var schema storage.Schema
	var err error
	schema, err = t.engine.GetTableSchema(name)
	if err != nil {
		return nil, err
	}

	// Unused variable, but kept for clarity
	_ = schema

	// Get the version store for this table
	versionStore, err := t.engine.GetVersionStore(name)
	if err != nil {
		return nil, err
	}

	// Get a table from the engine's pool
	mvccTable := t.engine.GetTableFromPool(t.id, versionStore)

	// Store the table in the map for future use
	t.tables[name] = mvccTable

	// Also update the fast path cache for quicker lookups
	t.lastTableName = name
	t.lastTable = mvccTable

	return mvccTable, nil
}

// ListTables returns a list of all tables
func (t *MVCCTransaction) ListTables() ([]string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get list of tables from engine
	engineTables, err := t.engine.ListTables()
	if err != nil {
		return nil, err
	}

	// Create a map to track all tables (for easier deduplication)
	allTables := make(map[string]bool)

	// Add engine tables
	for _, table := range engineTables {
		allTables[table] = true
	}

	// Add tables created in this transaction
	for tableName := range t.tables {
		allTables[tableName] = true
	}

	// Convert map keys to slice
	result := make([]string, 0, len(allTables))
	for tableName := range allTables {
		result = append(result, tableName)
	}

	return result, nil
}

// CreateTableIndex creates a new index on a table
func (t *MVCCTransaction) CreateTableIndex(tableName string, indexName string, columns []string, isUnique bool) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table implements the necessary methods for indexing
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support indexing")
	}

	// Create the index
	return mvccTable.CreateIndex(indexName, columns, isUnique)
}

// DropTableIndex drops an index from a table
func (t *MVCCTransaction) DropTableIndex(tableName string, indexName string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table implements the necessary methods for indexing
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support indexing")
	}

	// Drop the index
	return mvccTable.DropIndex(indexName)
}

// CreateTableColumnarIndex creates a columnar index on a table column
// These indexes provide HTAP (Hybrid Transactional/Analytical Processing) capabilities
// If customName is provided, it will be used as the index name instead of the auto-generated one
func (t *MVCCTransaction) CreateTableColumnarIndex(tableName string, columnName string, isUnique bool, customName ...string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table is an MVCC table
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support columnar indexing")
	}

	// Create the columnar index, passing the custom name if provided
	if len(customName) > 0 && customName[0] != "" {
		return mvccTable.CreateColumnarIndex(columnName, isUnique, customName[0])
	}

	// Create with default name
	return mvccTable.CreateColumnarIndex(columnName, isUnique)
}

// DropTableColumnarIndex drops a columnar index from a table column
func (t *MVCCTransaction) DropTableColumnarIndex(tableName string, columnName string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table is an MVCC table
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support columnar indexing")
	}

	// Use MVCCTable's drop columnar index method
	return mvccTable.DropColumnarIndex(columnName)
}
