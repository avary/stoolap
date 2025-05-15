package sql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

var (
	interfaceSlicePool = &sync.Pool{
		New: func() interface{} {
			p := make([]interface{}, 0, 16)
			return &p // Pre-allocate with reasonable capacity
		},
	}

	stringSlicePool = &sync.Pool{
		New: func() interface{} {
			p := make([]string, 0, 16)
			return &p // Pre-allocate with reasonable capacity
		},
	}
)

// executeInsertWithContext executes an INSERT statement
// Returns the number of rows affected and the last insert ID (for auto-increment columns)
func (e *Executor) executeInsertWithContext(ctx context.Context, tx storage.Transaction, stmt *parser.InsertStatement) (int64, int64, error) {
	// Extract table name from the table expression
	var tableName string

	// Handle different types of table expressions
	if stmt.TableName != nil {
		tableName = stmt.TableName.Value
	} else {
		return 0, 0, fmt.Errorf("missing table name in INSERT statement")
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return 0, 0, err
	}

	if !exists {
		return 0, 0, storage.ErrTableNotFound
	}

	// Get the table schema
	schema, err := e.engine.GetTableSchema(tableName)
	if err != nil {
		return 0, 0, err
	}

	cp := stringSlicePool.Get().(*[]string)
	columnNames := *cp
	if cap(columnNames) < len(stmt.Columns) {
		// Resize the slice if necessary
		columnNames = make([]string, len(stmt.Columns))
	} else {
		columnNames = columnNames[:len(stmt.Columns)]
	}

	defer func() {
		*cp = columnNames[:0]   // Reset the slice in the pool
		stringSlicePool.Put(cp) // Return to pool
	}()

	// Create a map of column names to indices for O(1) lookup
	columnMap := make(map[string]int, len(schema.Columns))
	for j, col := range schema.Columns {
		// Store both exact case and lowercase for case-insensitive lookup
		columnMap[col.Name] = j
		columnMap[strings.ToLower(col.Name)] = j
	}

	// Extract column names
	for i, col := range stmt.Columns {
		columnNames[i] = col.Value
	}

	// If no columns were specified, use all columns
	if len(columnNames) == 0 {
		if cap(columnNames) < len(schema.Columns) {
			// Resize the slice if necessary
			columnNames = make([]string, len(schema.Columns))
		} else {
			columnNames = columnNames[:len(schema.Columns)]
		}

		for i, col := range schema.Columns {
			columnNames[i] = col.Name
		}
	}

	// Process the values - support multi-row insert
	if len(stmt.Values) == 0 {
		return 0, 0, fmt.Errorf("no values provided for INSERT")
	}

	// Get the table to insert rows
	table, err := tx.GetTable(tableName)
	if err != nil {
		return 0, 0, err
	}

	// Extract parameter object once outside the loop
	var ps *parameter
	if pCtx := ctx.Value(psContextKey); pCtx != nil {
		ps, _ = pCtx.(*parameter)
	}

	// Optimize for batch insertion if we have multiple rows
	if len(stmt.Values) > 1 {
		// Check if table supports batch operations (MVCCTable)
		mvccTable, isMVCC := table.(*mvcc.MVCCTable)

		if isMVCC {
			// Pre-allocate all rows for batch processing
			rows := make([]storage.Row, 0, len(stmt.Values))

			// Process each row of values
			for rowIndex, rowExprs := range stmt.Values {
				if len(rowExprs) == 0 {
					return int64(rowIndex), 0, fmt.Errorf("empty values in row %d", rowIndex+1)
				}

				// Check if column count matches value count
				if len(columnNames) != len(rowExprs) {
					return int64(rowIndex), 0, fmt.Errorf("column count (%d) does not match value count (%d) in row %d",
						len(columnNames), len(rowExprs), rowIndex+1)
				}

				// Extract values from the expressions for this row
				cp := interfaceSlicePool.Get().(*[]interface{})
				columnValues := *cp
				if cap(columnValues) < len(rowExprs) {
					// Resize the slice if necessary
					columnValues = make([]interface{}, len(rowExprs))
				} else {
					columnValues = columnValues[:len(rowExprs)]
				}

				defer func() {
					*cp = columnValues[:0]     // Reset the slice in the pool
					interfaceSlicePool.Put(cp) // Return to pool
				}()

				for i, expr := range rowExprs {
					switch e := expr.(type) {
					case *parser.Parameter:
						if ps != nil {
							// Get the parameter value
							nm := ps.GetValue(e)
							columnValues[i] = nm.Value
						}
					case *parser.IntegerLiteral:
						columnValues[i] = e.Value
					case *parser.FloatLiteral:
						columnValues[i] = e.Value
					case *parser.StringLiteral:
						columnValues[i] = e.Value
					case *parser.BooleanLiteral:
						columnValues[i] = e.Value
					case *parser.NullLiteral:
						columnValues[i] = nil
					default:
						return int64(rowIndex), 0, fmt.Errorf("unsupported value type in row %d: %T", rowIndex+1, expr)
					}
				}

				// Create a row with the values in the correct order
				row := make(storage.Row, len(schema.Columns))

				// Initialize null columns
				for i := range row {
					// Set explicit NULL values for columns not included in the INSERT
					if !slices.Contains(columnNames, schema.Columns[i].Name) {
						row[i] = storage.ValueToColumnValue(nil, schema.Columns[i].Type)
					}
				}

				// Set the values for the specified columns
				for i, colName := range columnNames {
					// O(1) lookup
					colIndex, exists := columnMap[colName]
					if !exists {
						// Try lowercase version for case-insensitive match
						colIndex, exists = columnMap[strings.ToLower(colName)]
						if !exists {
							return int64(rowIndex), 0, fmt.Errorf("column not found: %s", colName)
						}
					}
					// Convert value to proper column value
					colType := schema.Columns[colIndex].Type
					row[colIndex] = storage.ValueToColumnValue(columnValues[i], colType)
				}

				// Add row to batch
				rows = append(rows, row)
			}

			// Insert all rows in a single batch operation
			if err := mvccTable.InsertBatch(rows); err != nil {
				return 0, 0, err
			}

			// Get the current auto-increment value after the batch insert
			lastInsertID := mvccTable.GetCurrentAutoIncrementValue()

			return int64(len(rows)), lastInsertID, nil
		}
	}

	// Fallback to single-row insertion for non-batch cases
	var totalRowsInserted int64

	for rowIndex, rowExprs := range stmt.Values {
		if len(rowExprs) == 0 {
			return totalRowsInserted, 0, fmt.Errorf("empty values in row %d", rowIndex+1)
		}

		cp := interfaceSlicePool.Get().(*[]interface{})
		columnValues := *cp
		if cap(columnValues) < len(rowExprs) {
			// Resize the slice if necessary
			columnValues = make([]interface{}, len(rowExprs))
		} else {
			columnValues = columnValues[:len(rowExprs)]
		}

		defer func() {
			*cp = columnValues[:0]     // Reset the slice in the pool
			interfaceSlicePool.Put(cp) // Return to pool
		}()

		for i, expr := range rowExprs {
			switch e := expr.(type) {
			case *parser.Parameter:
				if ps != nil {
					// Get the parameter value
					nm := ps.GetValue(e)
					columnValues[i] = nm.Value
				}
			case *parser.IntegerLiteral:
				columnValues[i] = e.Value
			case *parser.FloatLiteral:
				columnValues[i] = e.Value
			case *parser.StringLiteral:
				columnValues[i] = e.Value
			case *parser.BooleanLiteral:
				columnValues[i] = e.Value
			case *parser.NullLiteral:
				columnValues[i] = nil
			default:
				return totalRowsInserted, 0, fmt.Errorf("unsupported value type: %T", expr)
			}
		}

		// Check if the number of columns match the number of values
		if len(columnNames) != len(columnValues) {
			return totalRowsInserted, 0, fmt.Errorf("column count (%d) does not match value count (%d)",
				len(columnNames), len(columnValues))
		}

		// Create a row with the values in the correct order
		row := make(storage.Row, len(schema.Columns))

		// Initialize null columns
		for i := range row {
			// Set explicit NULL values
			if !slices.Contains(columnNames, schema.Columns[i].Name) {
				row[i] = storage.ValueToColumnValue(nil, schema.Columns[i].Type)
			}
		}

		// Set the values for the specified columns
		for i, colName := range columnNames {
			// O(1) lookup instead of O(n) scan
			colIndex, exists := columnMap[colName]
			if !exists {
				// Try lowercase version for case-insensitive match
				colIndex, exists = columnMap[strings.ToLower(colName)]
				if !exists {
					return totalRowsInserted, 0, fmt.Errorf("column not found: %s", colName)
				}
			}
			// Convert raw value to proper column value
			colType := schema.Columns[colIndex].Type
			row[colIndex] = storage.ValueToColumnValue(columnValues[i], colType)
		}

		// Try to insert the row
		err = table.Insert(row)
		if err != nil {
			// If we have ON DUPLICATE KEY UPDATE clause and we get a unique constraint violation
			if stmt.OnDuplicate {
				var pkErr *storage.ErrPrimaryKeyConstraint
				var uniqueErr *storage.ErrUniqueConstraint

				if errors.As(err, &pkErr) || errors.As(err, &uniqueErr) {
					// Create a search expression to find the duplicate row
					var searchExpr storage.Expression

					// Case 1: Primary key constraint violation - we know exactly which row ID to find
					if errors.As(err, &pkErr) {
						// Find primary key column to create the search expression
						for _, col := range schema.Columns {
							if col.PrimaryKey {
								searchExpr = expression.NewSimpleExpression(col.Name, storage.EQ, pkErr.RowID)
								searchExpr.PrepareForSchema(schema)
								break
							}
						}
					}

					// Case 2: Unique constraint violation - we have column name and value
					if searchExpr == nil && errors.As(err, &uniqueErr) {
						searchExpr = expression.NewSimpleExpression(uniqueErr.Column, storage.EQ, uniqueErr.Value.AsInterface())
						searchExpr.PrepareForSchema(schema)
					}

					// If we found a valid search expression, find and update the row
					if searchExpr != nil {
						// Get column indices for all columns
						colIndices := make([]int, len(schema.Columns))
						for i := range colIndices {
							colIndices[i] = i
						}

						// Scan for the duplicate row
						scanner, scanErr := table.Scan(colIndices, searchExpr)
						if scanErr == nil {
							defer scanner.Close()

							// If we found a row
							if scanner.Next() {
								// Create updater function for applying the ON DUPLICATE KEY UPDATE
								updaterFn := func(oldRow storage.Row) (storage.Row, bool) {
									// Create a new row as a copy of the old row
									newRow := make(storage.Row, len(oldRow))
									copy(newRow, oldRow)

									// Apply the updates
									for i, updateColumn := range stmt.UpdateColumns {
										colName := updateColumn.Value
										expr := stmt.UpdateExpressions[i]

										// Find the column index
										colIndex := -1
										for j, col := range schema.Columns {
											if col.Name == colName {
												colIndex = j
												break
											}
										}

										if colIndex != -1 {
											var updateValue interface{}

											// Evaluate the expression for the update
											switch e := expr.(type) {
											case *parser.Parameter:
												if ps != nil {
													nm := ps.GetValue(e)
													updateValue = nm.Value
												}
											case *parser.IntegerLiteral:
												updateValue = e.Value
											case *parser.FloatLiteral:
												updateValue = e.Value
											case *parser.StringLiteral:
												updateValue = e.Value
											case *parser.BooleanLiteral:
												updateValue = e.Value
											case *parser.NullLiteral:
												updateValue = nil
											}

											// Set the new value
											colType := schema.Columns[colIndex].Type
											newRow[colIndex] = storage.ValueToColumnValue(updateValue, colType)
										}
									}

									return newRow, false
								}

								// Update the row using the Update API
								_, updateErr := table.Update(searchExpr, updaterFn)
								if updateErr == nil {
									totalRowsInserted++
									continue
								}
							}
						}
					}

					// If we couldn't handle the duplicate key properly, return the original error
					return totalRowsInserted, 0, err
				}
			}
			// For any other error, return it without ON DUPLICATE KEY handling
			return totalRowsInserted, 0, err
		}

		// Normal insert success
		totalRowsInserted++
	}

	// Get the current auto-increment value after all inserts
	var lastInsertID int64 = 0
	if len(stmt.Values) > 0 {
		if mvccTable, ok := table.(*mvcc.MVCCTable); ok {
			lastInsertID = mvccTable.GetCurrentAutoIncrementValue()
		}
	}

	return totalRowsInserted, lastInsertID, nil
}

// executeUpdate executes an UPDATE statement
func (e *Executor) executeUpdateWithContext(ctx context.Context, tx storage.Transaction, stmt *parser.UpdateStatement) (int64, error) {
	// Extract table name from the table expression
	var tableName string

	// Handle different types of table expressions
	if stmt.TableName != nil {
		tableName = stmt.TableName.Value
	} else {
		return 0, fmt.Errorf("missing table name in UPDATE statement")
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, storage.ErrTableNotFound
	}

	// Get the table
	table, err := tx.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Get the schema to know the column types
	schema, err := e.engine.GetTableSchema(tableName)
	if err != nil {
		return 0, err
	}

	// Create a map of column names to indices and types for O(1) lookup
	columnMap := make(map[string]struct {
		Index int
		Type  storage.DataType
	}, len(schema.Columns))

	for i, col := range schema.Columns {
		// Store both exact case and lowercase for case-insensitive lookup
		columnMap[col.Name] = struct {
			Index int
			Type  storage.DataType
		}{
			Index: i,
			Type:  col.Type,
		}
		columnMap[strings.ToLower(col.Name)] = struct {
			Index int
			Type  storage.DataType
		}{
			Index: i,
			Type:  col.Type,
		}
	}

	// Extract parameter object once outside the loop
	var ps *parameter
	if pCtx := ctx.Value(psContextKey); pCtx != nil {
		ps, _ = pCtx.(*parameter)
	}

	// Create a setter function that updates the values
	setter := func(row storage.Row) (storage.Row, bool) {
		// Apply updates
		for colName, expr := range stmt.Updates {
			// Find the column using O(1) map lookup instead of O(n) scan
			colInfo, exists := columnMap[colName]
			if !exists {
				// Try lowercase version for case-insensitive match
				colInfo, exists = columnMap[strings.ToLower(colName)]
				if !exists {
					continue // Skip unknown column
				}
			}

			colIndex := colInfo.Index
			colType := colInfo.Type

			// Extract value from expression
			var value interface{}
			switch e := expr.(type) {
			case *parser.Parameter:
				if ps != nil {
					// Get the parameter value
					nm := ps.GetValue(e)
					value = nm.Value
				}
			case *parser.IntegerLiteral:
				value = e.Value
			case *parser.FloatLiteral:
				value = e.Value
			case *parser.StringLiteral:
				value = e.Value
			case *parser.BooleanLiteral:
				value = e.Value
			case *parser.NullLiteral:
				value = nil
			default:
				continue // Skip unsupported types
			}

			// Convert to column value and update
			row[colIndex] = storage.ValueToColumnValue(value, colType)
		}

		return row, true
	}

	// Create a storage-level expression from the SQL WHERE clause
	var updateExpr storage.Expression
	if stmt.Where != nil {
		// Convert the SQL WHERE expression to a storage-level expression
		updateExpr = createWhereExpression(ctx, stmt.Where, e.functionRegistry)
		updateExpr.PrepareForSchema(schema)
	} else {
		// If no WHERE clause, update all rows - use a simple expression that always returns true
		updateExpr = nil
	}

	// Execute a single UPDATE operation with the WHERE expression
	// and our setter function that applies column updates.
	// The Update method in the storage layer will handle the expression evaluation
	// and visibility concerns, making this much more efficient for large tables.
	count, err := table.Update(updateExpr, setter)
	if err != nil {
		return 0, fmt.Errorf("error executing UPDATE: %w", err)
	}
	return int64(count), nil
}

// executeDeleteWithContext executes a DELETE statement
func (e *Executor) executeDeleteWithContext(ctx context.Context, tx storage.Transaction, stmt *parser.DeleteStatement) (int64, error) {
	// Extract table name from the table expression
	var tableName string

	// Handle different types of table expressions
	if stmt.TableName != nil {
		tableName = stmt.TableName.Value
	} else {
		return 0, fmt.Errorf("missing table name in DELETE statement")
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, storage.ErrTableNotFound
	}

	// Get the table
	table, err := tx.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Get the schema to know the column types
	schema, err := e.engine.GetTableSchema(tableName)
	if err != nil {
		return 0, err
	}

	// Create a WHERE expression directly from the SQL WHERE clause
	var deleteExpr storage.Expression
	if stmt.Where != nil {
		// Convert the SQL WHERE expression to a storage-level expression
		deleteExpr = createWhereExpression(ctx, stmt.Where, e.functionRegistry)
		deleteExpr.PrepareForSchema(schema)
	} else {
		// If no WHERE clause, delete all rows - use a simple expression that always returns true
		deleteExpr = nil
	}

	// Execute the deletion directly using the WHERE expression at the storage layer
	// This avoids materializing rows in memory and is much more efficient
	rowsDeleted, err := table.Delete(deleteExpr)
	if err != nil {
		return 0, fmt.Errorf("error executing DELETE: %w", err)
	}

	return int64(rowsDeleted), nil
}

// createWhereExpression creates a storage.Expression from a parser.Expression
func createWhereExpression(ctx context.Context, expr parser.Expression, registry contract.FunctionRegistry) storage.Expression {
	if expr == nil {
		return nil
	}

	// Handle NOT expressions (implemented as PrefixExpression with operator "NOT")
	if prefixExpr, ok := expr.(*parser.PrefixExpression); ok && prefixExpr.Operator == "NOT" {
		// Recursively process the inner expression
		innerExpr := createWhereExpression(ctx, prefixExpr.Right, registry)

		// If we successfully created a storage expression, wrap it in a NOT
		if innerExpr != nil {
			return expression.NewNotExpression(innerExpr)
		}
		return nil
	}

	// For binary comparison operations, we can optimize by creating direct storage expressions
	if binaryExpr, ok := expr.(*parser.InfixExpression); ok {
		// Special handling for CAST expressions in comparisons
		if castExpr, ok := binaryExpr.Left.(*parser.CastExpression); ok {
			// Handle CAST(column AS type) operator value
			if ident, ok := castExpr.Expr.(*parser.Identifier); ok {
				// Extract the column name
				colName := ident.Value

				// Get the target data type
				var targetType storage.DataType
				switch strings.ToUpper(castExpr.TypeName) {
				case "INTEGER", "INT":
					targetType = storage.INTEGER
				case "FLOAT", "REAL", "DOUBLE":
					targetType = storage.FLOAT
				case "TEXT", "STRING", "VARCHAR":
					targetType = storage.TEXT
				case "BOOLEAN", "BOOL":
					targetType = storage.BOOLEAN
				case "TIMESTAMP":
					targetType = storage.TIMESTAMP
				case "JSON":
					targetType = storage.JSON
				default:
					return nil
				}

				// Create a cast expression
				castStorageExpr := expression.NewCastExpression(colName, targetType)

				// The value to compare to
				var value interface{}
				if isLiteral(binaryExpr.Right) {
					value = getLiteralValue(ctx, binaryExpr.Right)
				} else {
					// If not a simple literal, delegate to default handling
					return nil
				}

				// Get the operator
				var operator storage.Operator
				switch binaryExpr.Operator {
				case ">":
					operator = storage.GT
				case ">=":
					operator = storage.GTE
				case "<":
					operator = storage.LT
				case "<=":
					operator = storage.LTE
				case "=":
					operator = storage.EQ
				case "!=", "<>":
					operator = storage.NE
				default:
					return nil
				}

				// Create a compound expression
				compoundExpr := &expression.CompoundExpression{
					CastExpr: castStorageExpr,
					Operator: operator,
					Value:    value,
				}
				return compoundExpr
			}
		}
		if binaryExpr.Operator == ">" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.GT, value)
		} else if binaryExpr.Operator == ">=" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.GTE, value)
		} else if binaryExpr.Operator == "<" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.LT, value)
		} else if binaryExpr.Operator == "<=" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.LTE, value)
		} else if binaryExpr.Operator == "=" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.EQ, value)
		} else if binaryExpr.Operator == "!=" && isColumnAndLiteral(ctx, binaryExpr) {
			colName, value := extractColumnAndValue(ctx, binaryExpr)
			return expression.NewSimpleExpression(colName, storage.NE, value)
		} else if binaryExpr.Operator == "BETWEEN" {
			// Extract column name
			colName := ""
			if col, ok := binaryExpr.Left.(*parser.Identifier); ok {
				colName = col.Value
			} else {
				return nil
			}

			// Extract the min and max values from the AND expression
			if andExpr, ok := binaryExpr.Right.(*parser.InfixExpression); ok && andExpr.Operator == "AND" {
				lowerValue := getLiteralValue(ctx, andExpr.Left)
				upperValue := getLiteralValue(ctx, andExpr.Right)

				// Create a BetweenExpression that handles both bounds
				if colName != "" && lowerValue != nil && upperValue != nil {
					betweenExpr := &expression.BetweenExpression{
						Column:     colName,
						LowerBound: lowerValue,
						UpperBound: upperValue,
						Inclusive:  true, // BETWEEN is inclusive on both ends
					}

					return betweenExpr
				}
			}

			// If we can't properly extract the BETWEEN values, return nil
			return nil
		} else if binaryExpr.Operator == "AND" {
			// For AND expressions, optimize both sides
			leftExpr := createWhereExpression(ctx, binaryExpr.Left, registry)
			rightExpr := createWhereExpression(ctx, binaryExpr.Right, registry)

			// If both sides could be optimized, create an AND expression
			if leftExpr != nil && rightExpr != nil {
				return &expression.AndExpression{
					Expressions: []storage.Expression{leftExpr, rightExpr},
				}
			}
		} else if binaryExpr.Operator == "OR" {
			// For OR expressions, optimize both sides
			leftExpr := createWhereExpression(ctx, binaryExpr.Left, registry)
			rightExpr := createWhereExpression(ctx, binaryExpr.Right, registry)

			// If both sides could be optimized, create an OR expression
			if leftExpr != nil && rightExpr != nil {
				return &expression.OrExpression{
					Expressions: []storage.Expression{leftExpr, rightExpr},
				}
			}
		}
	}

	// Special handling for BETWEEN expressions
	if betweenExpr, ok := expr.(*parser.BetweenExpression); ok {
		// Extract column name
		var columnName string
		if col, ok := betweenExpr.Expr.(*parser.Identifier); ok {
			columnName = col.Value
		} else {
			return nil
		}

		// Extract lower and upper bounds
		lowerValue := getLiteralValue(ctx, betweenExpr.Lower)
		upperValue := getLiteralValue(ctx, betweenExpr.Upper)

		if lowerValue != nil && upperValue != nil {
			// Create a BetweenExpression directly from the parser expression
			betweenStorageExpr := &expression.BetweenExpression{
				Column:     columnName,
				LowerBound: lowerValue,
				UpperBound: upperValue,
				Inclusive:  true, // BETWEEN is always inclusive
			}

			return betweenStorageExpr
		}

		return nil
	}

	// Special handling for CAST expressions
	if castExpr, ok := expr.(*parser.CastExpression); ok {
		// Handle CAST(column AS type)
		if ident, ok := castExpr.Expr.(*parser.Identifier); ok {
			// Extract the target data type
			var targetType storage.DataType
			switch strings.ToUpper(castExpr.TypeName) {
			case "INTEGER", "INT":
				targetType = storage.INTEGER
			case "FLOAT", "REAL", "DOUBLE":
				targetType = storage.FLOAT
			case "TEXT", "STRING", "VARCHAR":
				targetType = storage.TEXT
			case "BOOLEAN", "BOOL":
				targetType = storage.BOOLEAN
			case "TIMESTAMP":
				targetType = storage.TIMESTAMP
			case "JSON":
				targetType = storage.JSON
			default:
				// Unsupported type, fall back to default handling
				return nil
			}

			// Create a CastExpression for the storage layer
			return expression.NewCastExpression(ident.Value, targetType)
		}
	}

	// Special handling for IS NULL and IS NOT NULL expressions
	if infix, ok := expr.(*parser.InfixExpression); ok {
		if infix.Operator == "IS" || infix.Operator == "IS NOT" {
			if colExpr, ok := infix.Left.(*parser.Identifier); ok {
				if _, ok := infix.Right.(*parser.NullLiteral); ok {
					if infix.Operator == "IS" {
						// IS NULL expression
						return expression.NewIsNullExpression(colExpr.Value)
					} else {
						// IS NOT NULL expression
						return expression.NewIsNotNullExpression(colExpr.Value)
					}
				}
			}
		}
	}

	// Special handling for IN expressions
	if inExpr, ok := expr.(*parser.InExpression); ok {
		// Extract column name from the left side
		var columnName string
		if col, ok := inExpr.Left.(*parser.Identifier); ok {
			columnName = col.Value
		} else {
			// Fall back to string representation if not a simple identifier
			columnName = inExpr.Left.String()
		}

		// Check if right side is an expression list
		if exprList, ok := inExpr.Right.(*parser.ExpressionList); ok {
			// Extract values from the expression list
			values := make([]interface{}, 0, len(exprList.Expressions))
			for _, e := range exprList.Expressions {
				switch v := e.(type) {
				case *parser.StringLiteral:
					values = append(values, v.Value)
				case *parser.IntegerLiteral:
					values = append(values, v.Value)
				case *parser.FloatLiteral:
					values = append(values, v.Value)
				case *parser.BooleanLiteral:
					values = append(values, v.Value)
				case *parser.Parameter:
					// Handle parameter binding if available
					if ps := getParameterFromContext(ctx); ps != nil {
						param := ps.GetValue(v)
						values = append(values, param.Value)
					}
				case *parser.NullLiteral:
					values = append(values, nil)
				}
			}

			// Create a direct in-list expression with the Not flag
			return &expression.InListExpression{
				Column: columnName,
				Values: values,
				Not:    inExpr.Not, // Pass the Not flag from the parser InExpression
			}
		}
	}

	return nil
}

// isColumnAndLiteral checks if an expression is a comparison between a column and a literal value
func isColumnAndLiteral(ctx context.Context, expr *parser.InfixExpression) bool {
	_ = ctx // Unused context parameter

	// Check if left is a column and right is a literal
	if _, isColLeft := expr.Left.(*parser.Identifier); isColLeft {
		return isLiteral(expr.Right)
	}

	// Check if right is a column and left is a literal
	if _, isColRight := expr.Right.(*parser.Identifier); isColRight {
		return isLiteral(expr.Left)
	}

	return false
}

// isLiteral checks if an expression is a literal value
func isLiteral(expr parser.Expression) bool {
	switch expr.(type) {
	case *parser.IntegerLiteral, *parser.FloatLiteral, *parser.StringLiteral, *parser.BooleanLiteral, *parser.Parameter:
		return true
	default:
		return false
	}
}

// extractColumnAndValue extracts the column name and literal value from a binary expression
func extractColumnAndValue(ctx context.Context, expr *parser.InfixExpression) (string, interface{}) {
	// Check if left is column and right is literal
	if colExpr, isColLeft := expr.Left.(*parser.Identifier); isColLeft && isLiteral(expr.Right) {
		return colExpr.Value, getLiteralValue(ctx, expr.Right)
	}

	// Check if right is column and left is literal
	if colExpr, isColRight := expr.Right.(*parser.Identifier); isColRight && isLiteral(expr.Left) {
		// For reversed comparisons, we need to adjust the operator
		// e.g., "5 > id" should be interpreted as "id < 5"
		return colExpr.Value, getLiteralValue(ctx, expr.Left)
	}

	// This should never happen if isColumnAndLiteral was called first
	return "", nil
}

// Core function to extract parameter from context - do this once per query processing
func getParameterFromContext(ctx context.Context) *parameter {
	if pCtx := ctx.Value(psContextKey); pCtx != nil {
		if ps, ok := pCtx.(*parameter); ok {
			return ps
		}
	}
	return nil
}

// getLiteralValue extracts the value from a literal expression
// Pass in the pre-extracted parameter object to avoid context lookups
func getLiteralValue(ctx context.Context, expr parser.Expression) interface{} {
	switch e := expr.(type) {
	case *parser.IntegerLiteral:
		return e.Value
	case *parser.FloatLiteral:
		return e.Value
	case *parser.StringLiteral:
		return e.Value
	case *parser.BooleanLiteral:
		return e.Value
	case *parser.Parameter:
		// Extract parameter just once if we need it
		ps := getParameterFromContext(ctx)
		if ps != nil {
			// Get the parameter value
			nm := ps.GetValue(e)
			return nm.Value
		}
		return nil
	default:
		return nil
	}
}
