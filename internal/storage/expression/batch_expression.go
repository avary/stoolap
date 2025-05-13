package expression

import (
	"github.com/stoolap/stoolap/internal/storage"
)

// BatchExpressionEvaluator is a high-performance expression evaluator
// that processes multiple rows at once with minimal overhead.
type BatchExpressionEvaluator struct {
	// The wrapped expression
	expr storage.Expression

	// Fast path handlers for common expression types
	simpleEvalFn    func(storage.Row) bool
	andEvalFn       func(storage.Row) bool
	orEvalFn        func(storage.Row) bool
	betweenEvalFn   func(storage.Row) bool
	inListEvalFn    func(storage.Row) bool
	nullCheckEvalFn func(storage.Row) bool

	// Schema information for fast column lookups
	columnIndices map[string]int

	// Configuration
	enableFastPath bool
}

// NewBatchExpressionEvaluator creates a new BatchExpressionEvaluator for an expression
func NewBatchExpressionEvaluator(expr storage.Expression, schema storage.Schema) *BatchExpressionEvaluator {
	evaluator := &BatchExpressionEvaluator{
		expr:           expr,
		columnIndices:  make(map[string]int, len(schema.Columns)),
		enableFastPath: true,
	}

	// Pre-build column index map
	for i, col := range schema.Columns {
		evaluator.columnIndices[col.Name] = i
	}

	// Analyze expression and prepare fast path handlers
	evaluator.prepareOptimizedHandlers(expr, schema)

	return evaluator
}

// prepareOptimizedHandlers analyzes the expression and creates specialized
// function handlers for common expression types
func (e *BatchExpressionEvaluator) prepareOptimizedHandlers(expr storage.Expression, schema storage.Schema) {
	// Handle the most common expression types
	switch typedExpr := expr.(type) {
	case *SimpleExpression:
		e.prepareSimpleExpressionHandler(typedExpr, schema)
	case *AndExpression:
		e.prepareAndExpressionHandler(typedExpr, schema)
	case *OrExpression:
		e.prepareOrExpressionHandler(typedExpr, schema)
	case *BetweenExpression:
		e.prepareBetweenExpressionHandler(typedExpr, schema)
	case *InListExpression:
		e.prepareInListExpressionHandler(typedExpr, schema)
	case *NullCheckExpression:
		e.prepareNullCheckExpressionHandler(typedExpr, schema)
	case *SchemaAwareExpression:
		// For SchemaAwareExpression, optimize its wrapped expression
		e.prepareOptimizedHandlers(typedExpr.Expr, schema)
	}
}

// prepareSimpleExpressionHandler creates an optimized handler for SimpleExpression
func (e *BatchExpressionEvaluator) prepareSimpleExpressionHandler(expr *SimpleExpression, schema storage.Schema) {
	// Find column index from schema
	colIndex := -1
	for i, col := range schema.Columns {
		if col.Name == expr.Column {
			colIndex = i
			break
		}
	}

	// If can't find column, fall back to standard evaluation
	if colIndex == -1 {
		return
	}

	// Create specialized function based on value type and operator
	switch expr.ValueType {
	case storage.TypeInteger:
		compValue := expr.Int64Value
		switch expr.Operator {
		case storage.EQ:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v == compValue
			}
		case storage.NE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v != compValue
			}
		case storage.GT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v > compValue
			}
		case storage.GTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v >= compValue
			}
		case storage.LT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v < compValue
			}
		case storage.LTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsInt64()
				if !ok {
					return false
				}
				return v <= compValue
			}
		}
	case storage.TypeFloat:
		compValue := expr.Float64Value
		switch expr.Operator {
		case storage.EQ:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v == compValue
			}
		case storage.NE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v != compValue
			}
		case storage.GT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v > compValue
			}
		case storage.GTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v >= compValue
			}
		case storage.LT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v < compValue
			}
		case storage.LTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsFloat64()
				if !ok {
					return false
				}
				return v <= compValue
			}
		}
	case storage.TypeString:
		compValue := expr.StringValue
		switch expr.Operator {
		case storage.EQ:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v == compValue
			}
		case storage.NE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v != compValue
			}
		case storage.GT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v > compValue
			}
		case storage.GTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v >= compValue
			}
		case storage.LT:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v < compValue
			}
		case storage.LTE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsString()
				if !ok {
					return false
				}
				return v <= compValue
			}
		}
	case storage.TypeBoolean:
		compValue := expr.BoolValue
		switch expr.Operator {
		case storage.EQ:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsBoolean()
				if !ok {
					return false
				}
				return v == compValue
			}
		case storage.NE:
			e.simpleEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}
				v, ok := row[colIndex].AsBoolean()
				if !ok {
					return false
				}
				return v != compValue
			}
		}
	}
}

// prepareAndExpressionHandler creates an optimized handler for AndExpression
func (e *BatchExpressionEvaluator) prepareAndExpressionHandler(expr *AndExpression, schema storage.Schema) {
	// Create batch evaluators for each child expression
	childEvaluators := make([]*BatchExpressionEvaluator, len(expr.Expressions))
	for i, childExpr := range expr.Expressions {
		childEvaluators[i] = NewBatchExpressionEvaluator(childExpr, schema)
	}

	// Create optimized AND function
	e.andEvalFn = func(row storage.Row) bool {
		for _, evaluator := range childEvaluators {
			if !evaluator.EvaluateFast(row) {
				return false // Short-circuit AND
			}
		}
		return true
	}
}

// prepareOrExpressionHandler creates an optimized handler for OrExpression
func (e *BatchExpressionEvaluator) prepareOrExpressionHandler(expr *OrExpression, schema storage.Schema) {
	// Create batch evaluators for each child expression
	childEvaluators := make([]*BatchExpressionEvaluator, len(expr.Expressions))
	for i, childExpr := range expr.Expressions {
		childEvaluators[i] = NewBatchExpressionEvaluator(childExpr, schema)
	}

	// Create optimized OR function
	e.orEvalFn = func(row storage.Row) bool {
		for _, evaluator := range childEvaluators {
			if evaluator.EvaluateFast(row) {
				return true // Short-circuit OR
			}
		}
		return false
	}
}

// prepareBetweenExpressionHandler creates optimized handler for BetweenExpression
func (e *BatchExpressionEvaluator) prepareBetweenExpressionHandler(expr *BetweenExpression, schema storage.Schema) {
	// Find column index from schema
	colIndex := -1
	for i, col := range schema.Columns {
		if col.Name == expr.Column {
			colIndex = i
			break
		}
	}

	// If can't find column, fall back to standard evaluation
	if colIndex == -1 {
		return
	}

	// Try to extract bounds for common types
	switch lower := expr.LowerBound.(type) {
	case int64:
		if upper, ok := expr.UpperBound.(int64); ok {
			if expr.Inclusive {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsInt64()
					if !ok {
						return false
					}
					return v >= lower && v <= upper
				}
			} else {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsInt64()
					if !ok {
						return false
					}
					return v > lower && v < upper
				}
			}
		}
	case float64:
		if upper, ok := expr.UpperBound.(float64); ok {
			if expr.Inclusive {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsFloat64()
					if !ok {
						return false
					}
					return v >= lower && v <= upper
				}
			} else {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsFloat64()
					if !ok {
						return false
					}
					return v > lower && v < upper
				}
			}
		}
	case string:
		if upper, ok := expr.UpperBound.(string); ok {
			if expr.Inclusive {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsString()
					if !ok {
						return false
					}
					return v >= lower && v <= upper
				}
			} else {
				e.betweenEvalFn = func(row storage.Row) bool {
					if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
						return false
					}
					v, ok := row[colIndex].AsString()
					if !ok {
						return false
					}
					return v > lower && v < upper
				}
			}
		}
	}
}

// prepareInListExpressionHandler creates optimized handler for InListExpression
func (e *BatchExpressionEvaluator) prepareInListExpressionHandler(expr *InListExpression, schema storage.Schema) {
	// Find column index from schema
	colIndex := -1
	for i, col := range schema.Columns {
		if col.Name == expr.Column {
			colIndex = i
			break
		}
	}

	// If can't find column, fall back to standard evaluation
	if colIndex == -1 {
		return
	}

	// Optimize for common integer IN lists by creating a lookup map
	if len(expr.Values) > 0 {
		// Check if all values are int64 compatible
		allInts := true
		intMap := make(map[int64]struct{}, len(expr.Values))

		for _, val := range expr.Values {
			switch v := val.(type) {
			case int:
				intMap[int64(v)] = struct{}{}
			case int64:
				intMap[v] = struct{}{}
			case float64:
				// Only use if it's a whole number
				if float64(int64(v)) == v {
					intMap[int64(v)] = struct{}{}
				} else {
					allInts = false
				}
			default:
				allInts = false
			}

			if !allInts {
				break
			}
		}

		if allInts {
			// Create optimized int64 lookup
			not := expr.Not
			e.inListEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}

				v, ok := row[colIndex].AsInt64()
				if !ok {
					return not // Not found for NOT IN is true
				}

				_, found := intMap[v]
				return found != not // XOR with NOT flag
			}
			return
		}

		// Similar optimization for string values
		allStrings := true
		strMap := make(map[string]struct{}, len(expr.Values))

		for _, val := range expr.Values {
			if str, ok := val.(string); ok {
				strMap[str] = struct{}{}
			} else {
				allStrings = false
				break
			}
		}

		if allStrings {
			// Create optimized string lookup
			not := expr.Not
			e.inListEvalFn = func(row storage.Row) bool {
				if colIndex >= len(row) || row[colIndex] == nil || row[colIndex].IsNull() {
					return false
				}

				v, ok := row[colIndex].AsString()
				if !ok {
					return not // Not found for NOT IN is true
				}

				_, found := strMap[v]
				return found != not // XOR with NOT flag
			}
			return
		}
	}
}

// prepareNullCheckExpressionHandler creates optimized handler for NullCheckExpression
func (e *BatchExpressionEvaluator) prepareNullCheckExpressionHandler(expr *NullCheckExpression, schema storage.Schema) {
	// Find column index from schema
	colIndex := -1
	for i, col := range schema.Columns {
		if col.Name == expr.GetColumnName() {
			colIndex = i
			break
		}
	}

	// If can't find column, fall back to standard evaluation
	if colIndex == -1 {
		return
	}

	isNull := expr.isNull
	e.nullCheckEvalFn = func(row storage.Row) bool {
		if colIndex >= len(row) {
			return isNull // Out of range is treated as NULL
		}
		colValue := row[colIndex]
		valueIsNull := colValue == nil || colValue.IsNull()
		return valueIsNull == isNull // XOR operation
	}
}

// EvaluateFast evaluates the expression against a row using the fastest available path
func (e *BatchExpressionEvaluator) EvaluateFast(row storage.Row) bool {
	// Use the appropriate fast path function if available
	if e.enableFastPath {
		if e.simpleEvalFn != nil {
			return e.simpleEvalFn(row)
		}
		if e.andEvalFn != nil {
			return e.andEvalFn(row)
		}
		if e.orEvalFn != nil {
			return e.orEvalFn(row)
		}
		if e.betweenEvalFn != nil {
			return e.betweenEvalFn(row)
		}
		if e.inListEvalFn != nil {
			return e.inListEvalFn(row)
		}
		if e.nullCheckEvalFn != nil {
			return e.nullCheckEvalFn(row)
		}
	}

	// Fall back to standard evaluation
	result, _ := e.expr.Evaluate(row)
	return result
}

// Evaluate implements the storage.Expression interface
func (e *BatchExpressionEvaluator) Evaluate(row storage.Row) (bool, error) {
	// For standard interface compliance, wrap the fast evaluation
	if e.enableFastPath {
		if e.simpleEvalFn != nil {
			return e.simpleEvalFn(row), nil
		}
		if e.andEvalFn != nil {
			return e.andEvalFn(row), nil
		}
		if e.orEvalFn != nil {
			return e.orEvalFn(row), nil
		}
		if e.betweenEvalFn != nil {
			return e.betweenEvalFn(row), nil
		}
		if e.inListEvalFn != nil {
			return e.inListEvalFn(row), nil
		}
		if e.nullCheckEvalFn != nil {
			return e.nullCheckEvalFn(row), nil
		}
	}

	// Fall back to standard evaluation
	return e.expr.Evaluate(row)
}

// GetColumnName returns the primary column name used in the expression
func (e *BatchExpressionEvaluator) GetColumnName() string {
	if colNamer, ok := e.expr.(interface{ GetColumnName() string }); ok {
		return colNamer.GetColumnName()
	}
	return ""
}

// GetValue returns the comparison value of the expression
func (e *BatchExpressionEvaluator) GetValue() interface{} {
	if valuer, ok := e.expr.(interface{ GetValue() interface{} }); ok {
		return valuer.GetValue()
	}
	return nil
}

// GetOperator returns the operator of the expression
func (e *BatchExpressionEvaluator) GetOperator() storage.Operator {
	if oper, ok := e.expr.(interface{ GetOperator() storage.Operator }); ok {
		return oper.GetOperator()
	}
	return storage.EQ
}

// CanUseIndex returns whether this expression can use an index
func (e *BatchExpressionEvaluator) CanUseIndex() bool {
	if indexer, ok := e.expr.(interface{ CanUseIndex() bool }); ok {
		return indexer.CanUseIndex()
	}
	return false
}

// WithAliases returns a new expression with column aliases applied
func (e *BatchExpressionEvaluator) WithAliases(aliases map[string]string) storage.Expression {
	if aliaser, ok := e.expr.(interface {
		WithAliases(map[string]string) storage.Expression
	}); ok {
		newExpr := aliaser.WithAliases(aliases)
		// Return a new evaluator with the aliased expression
		return NewBatchExpressionEvaluator(newExpr, e.getSchema())
	}
	return e
}

// getSchema attempts to reconstruct schema from stored column indices
func (e *BatchExpressionEvaluator) getSchema() storage.Schema {
	// Create schema with empty columns, to be filled based on the column indices
	schema := storage.Schema{
		Columns: make([]storage.SchemaColumn, len(e.columnIndices)),
	}

	// Fill in column definitions from column indices map
	for colName, idx := range e.columnIndices {
		if idx >= 0 && idx < len(schema.Columns) {
			schema.Columns[idx] = storage.SchemaColumn{
				Name: colName,
				// Type is unknown here, but that's fine for most purposes
				Type: storage.NULL,
			}
		}
	}

	return schema
}

// EvaluateBatch evaluates the expression for multiple rows at once,
// returning a bitmap of which rows satisfied the condition
func (e *BatchExpressionEvaluator) EvaluateBatch(rows []storage.Row) []bool {
	result := make([]bool, len(rows))
	for i, row := range rows {
		result[i] = e.EvaluateFast(row)
	}
	return result
}
