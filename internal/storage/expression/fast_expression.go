package expression

import (
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// FastSimpleExpression is a highly optimized version of SimpleExpression
// specifically for primary key operations on common data types
type FastSimpleExpression struct {
	Column   string           // Name of the column
	Operator storage.Operator // Comparison operator

	// Different value types are stored in their native form to avoid interface{} overhead
	Int64Value   int64
	Float64Value float64
	StringValue  string
	BoolValue    bool
	TimeValue    time.Time
	ValueType    storage.DataType
	ValueIsNil   bool
	ColIndex     int  // Column index for fast lookup
	IndexPrepped bool // Whether we've prepared column index mapping
}

// NewFastSimpleExpression creates a new optimized simple expression
func NewFastSimpleExpression(column string, operator storage.Operator, value interface{}) *FastSimpleExpression {
	expr := &FastSimpleExpression{
		Column:       column,
		Operator:     operator,
		IndexPrepped: false,
		ColIndex:     -1,
		ValueIsNil:   value == nil,
	}

	// Store the value in its native type to avoid interface{} overhead
	if !expr.ValueIsNil {
		switch v := value.(type) {
		case int:
			expr.ValueType = storage.TypeInteger
			expr.Int64Value = int64(v)
		case int64:
			expr.ValueType = storage.TypeInteger
			expr.Int64Value = v
		case float64:
			expr.ValueType = storage.TypeFloat
			expr.Float64Value = v
		case string:
			expr.ValueType = storage.TypeString
			expr.StringValue = v
		case bool:
			expr.ValueType = storage.TypeBoolean
			expr.BoolValue = v
		case time.Time:
			expr.ValueType = storage.TypeTimestamp
			expr.TimeValue = v
		default:
			// Fall back to string for other types
			expr.ValueType = storage.TypeString
			expr.StringValue = storage.ConvertValueToString(value)
		}
	}

	return expr
}

// PrepareForSchema optimizes the expression for a specific schema by calculating
// column indices in advance for fast evaluation
func (e *FastSimpleExpression) PrepareForSchema(schema storage.Schema) {
	// Find the column index to optimize lookup
	for i, col := range schema.Columns {
		if col.Name == e.Column {
			e.ColIndex = i
			break
		}
	}
	e.IndexPrepped = true
}

// Evaluate implements the storage.Expression interface
// This is a highly optimized implementation that avoids most allocations
func (e *FastSimpleExpression) Evaluate(row storage.Row) (bool, error) {
	// Quick validations
	if len(row) == 0 {
		return false, nil
	}

	// Use cached column index if available
	colIdx := e.ColIndex

	// Return false if column not found
	if colIdx < 0 || colIdx >= len(row) {
		return false, nil
	}

	// Get the column value
	colVal := row[colIdx]
	if colVal == nil {
		// Handle NULL checks
		if e.Operator == storage.ISNULL {
			return true, nil
		} else if e.Operator == storage.ISNOTNULL {
			return false, nil
		}
		return false, nil
	}

	// Handle NULL check operators
	if e.Operator == storage.ISNULL {
		return colVal.IsNull(), nil
	} else if e.Operator == storage.ISNOTNULL {
		return !colVal.IsNull(), nil
	}

	// Skip processing if the column value is NULL (except for NULL checks)
	if colVal.IsNull() {
		return false, nil
	}

	// Fast path evaluation based on column data type
	switch colVal.Type() {
	case storage.TypeInteger:
		return e.evaluateInteger(colVal)
	case storage.TypeFloat:
		return e.evaluateFloat(colVal)
	case storage.TypeString:
		return e.evaluateString(colVal)
	case storage.TypeBoolean:
		return e.evaluateBoolean(colVal)
	case storage.TypeTimestamp:
		return e.evaluateTime(colVal)
	default:
		// For other types, fall back to string comparison
		return e.evaluateString(colVal)
	}
}

// evaluateInteger handles integer comparisons efficiently
func (e *FastSimpleExpression) evaluateInteger(colVal storage.ColumnValue) (bool, error) {
	v, ok := colVal.AsInt64()
	if !ok {
		return false, nil
	}

	// When the expression value is the right type, do direct comparison
	if e.ValueType == storage.TypeInteger {
		switch e.Operator {
		case storage.EQ:
			return v == e.Int64Value, nil
		case storage.NE:
			return v != e.Int64Value, nil
		case storage.GT:
			return v > e.Int64Value, nil
		case storage.GTE:
			return v >= e.Int64Value, nil
		case storage.LT:
			return v < e.Int64Value, nil
		case storage.LTE:
			return v <= e.Int64Value, nil
		}
	} else if e.ValueType == storage.TypeFloat {
		// Handle conversion for float expression value
		floatVal := float64(v)
		switch e.Operator {
		case storage.EQ:
			return floatVal == e.Float64Value, nil
		case storage.NE:
			return floatVal != e.Float64Value, nil
		case storage.GT:
			return floatVal > e.Float64Value, nil
		case storage.GTE:
			return floatVal >= e.Float64Value, nil
		case storage.LT:
			return floatVal < e.Float64Value, nil
		case storage.LTE:
			return floatVal <= e.Float64Value, nil
		}
	}

	// Fallback to string comparison for other value types
	return false, nil
}

// evaluateFloat handles float comparisons efficiently
func (e *FastSimpleExpression) evaluateFloat(colVal storage.ColumnValue) (bool, error) {
	v, ok := colVal.AsFloat64()
	if !ok {
		return false, nil
	}

	// Handle different expression value types
	if e.ValueType == storage.TypeFloat {
		switch e.Operator {
		case storage.EQ:
			return v == e.Float64Value, nil
		case storage.NE:
			return v != e.Float64Value, nil
		case storage.GT:
			return v > e.Float64Value, nil
		case storage.GTE:
			return v >= e.Float64Value, nil
		case storage.LT:
			return v < e.Float64Value, nil
		case storage.LTE:
			return v <= e.Float64Value, nil
		}
	} else if e.ValueType == storage.TypeInteger {
		// Handle conversion for integer expression value
		floatVal := float64(e.Int64Value)
		switch e.Operator {
		case storage.EQ:
			return v == floatVal, nil
		case storage.NE:
			return v != floatVal, nil
		case storage.GT:
			return v > floatVal, nil
		case storage.GTE:
			return v >= floatVal, nil
		case storage.LT:
			return v < floatVal, nil
		case storage.LTE:
			return v <= floatVal, nil
		}
	}

	// Fallback to string comparison for other value types
	return false, nil
}

// evaluateString handles string comparisons efficiently
func (e *FastSimpleExpression) evaluateString(colVal storage.ColumnValue) (bool, error) {
	v, ok := colVal.AsString()
	if !ok {
		return false, nil
	}

	if e.ValueType == storage.TypeString {
		switch e.Operator {
		case storage.EQ:
			return v == e.StringValue, nil
		case storage.NE:
			return v != e.StringValue, nil
		case storage.GT:
			return v > e.StringValue, nil
		case storage.GTE:
			return v >= e.StringValue, nil
		case storage.LT:
			return v < e.StringValue, nil
		case storage.LTE:
			return v <= e.StringValue, nil
		}
	}

	// For other value types, we'd need to convert them to string
	return false, nil
}

// evaluateBoolean handles boolean comparisons efficiently
func (e *FastSimpleExpression) evaluateBoolean(colVal storage.ColumnValue) (bool, error) {
	v, ok := colVal.AsBoolean()
	if !ok {
		return false, nil
	}

	if e.ValueType == storage.TypeBoolean {
		switch e.Operator {
		case storage.EQ:
			return v == e.BoolValue, nil
		case storage.NE:
			return v != e.BoolValue, nil
		}
	}

	return false, nil
}

// evaluateTime handles time/date/timestamp comparisons efficiently
func (e *FastSimpleExpression) evaluateTime(colVal storage.ColumnValue) (bool, error) {
	v, ok := colVal.AsTimestamp()
	if !ok {
		return false, nil
	}

	if e.ValueType == storage.TypeTimestamp {
		switch e.Operator {
		case storage.EQ:
			return v.Equal(e.TimeValue), nil
		case storage.NE:
			return !v.Equal(e.TimeValue), nil
		case storage.GT:
			return v.After(e.TimeValue), nil
		case storage.GTE:
			return v.After(e.TimeValue) || v.Equal(e.TimeValue), nil
		case storage.LT:
			return v.Before(e.TimeValue), nil
		case storage.LTE:
			return v.Before(e.TimeValue) || v.Equal(e.TimeValue), nil
		}
	}

	return false, nil
}

// EvaluateFast is an ultra-optimized version of Evaluate that avoids interface method calls
// and type assertions where possible, for the critical path in query processing
func (e *FastSimpleExpression) EvaluateFast(row storage.Row) bool {
	// Use cached column index if available
	colIdx := e.ColIndex
	if !e.IndexPrepped || colIdx < 0 || colIdx >= len(row) {
		// Fallback to slower path
		result, _ := e.Evaluate(row)
		return result
	}

	// Get the column value
	colVal := row[colIdx]
	if colVal == nil {
		// Handle NULL checks
		if e.Operator == storage.ISNULL {
			return true
		} else if e.Operator == storage.ISNOTNULL {
			return false
		}
		return false
	}

	// Handle NULL check operators
	if e.Operator == storage.ISNULL {
		return colVal.IsNull()
	} else if e.Operator == storage.ISNOTNULL {
		return !colVal.IsNull()
	}

	// Skip processing if the column value is NULL (except for NULL checks)
	if colVal.IsNull() {
		return false
	}

	// Early return for fast equality check without type conversion
	// This is a critical optimization for primary key lookups
	if e.Operator == storage.EQ && e.ValueType == colVal.Type() {
		if dv, ok := colVal.(*storage.DirectValue); ok && dv != nil {
			// Direct comparison for DirectValue objects - no type conversion required
			switch e.ValueType {
			case storage.TypeInteger:
				return dv.AsInterface() == e.Int64Value
			case storage.TypeFloat:
				return dv.AsInterface() == e.Float64Value
			case storage.TypeString:
				return dv.AsInterface() == e.StringValue
			case storage.TypeBoolean:
				return dv.AsInterface() == e.BoolValue
			}
		}
	}

	// Fast path for integer comparisons (most common case)
	if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
		v, ok := colVal.AsInt64()
		if !ok {
			return false
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.Int64Value
		case storage.NE:
			return v != e.Int64Value
		case storage.GT:
			return v > e.Int64Value
		case storage.GTE:
			return v >= e.Int64Value
		case storage.LT:
			return v < e.Int64Value
		case storage.LTE:
			return v <= e.Int64Value
		}
		return false
	}

	// Fast path for float comparisons
	if e.ValueType == storage.TypeFloat && colVal.Type() == storage.TypeFloat {
		v, ok := colVal.AsFloat64()
		if !ok {
			return false
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.Float64Value
		case storage.NE:
			return v != e.Float64Value
		case storage.GT:
			return v > e.Float64Value
		case storage.GTE:
			return v >= e.Float64Value
		case storage.LT:
			return v < e.Float64Value
		case storage.LTE:
			return v <= e.Float64Value
		}
		return false
	}

	// Fast path for string comparisons
	if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
		v, ok := colVal.AsString()
		if !ok {
			return false
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.StringValue
		case storage.NE:
			return v != e.StringValue
		case storage.GT:
			return v > e.StringValue
		case storage.GTE:
			return v >= e.StringValue
		case storage.LT:
			return v < e.StringValue
		case storage.LTE:
			return v <= e.StringValue
		}
		return false
	}

	// Fast path for boolean comparisons
	if e.ValueType == storage.TypeBoolean && colVal.Type() == storage.TypeBoolean {
		v, ok := colVal.AsBoolean()
		if !ok {
			return false
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.BoolValue
		case storage.NE:
			return v != e.BoolValue
		}
		return false
	}

	// Fast path for timestamp comparisons
	if e.ValueType == storage.TypeTimestamp && colVal.Type() == storage.TypeTimestamp {
		v, ok := colVal.AsTimestamp()
		if !ok {
			return false
		}

		switch e.Operator {
		case storage.EQ:
			return v.Equal(e.TimeValue)
		case storage.NE:
			return !v.Equal(e.TimeValue)
		case storage.GT:
			return v.After(e.TimeValue)
		case storage.GTE:
			return v.After(e.TimeValue) || v.Equal(e.TimeValue)
		case storage.LT:
			return v.Before(e.TimeValue)
		case storage.LTE:
			return v.Before(e.TimeValue) || v.Equal(e.TimeValue)
		}
		return false
	}

	// Optimized handling for mixed numeric types
	if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
		(colVal.Type() == storage.TypeInteger || colVal.Type() == storage.TypeFloat) {

		var colFloat, constFloat float64
		var ok bool

		// Convert column value to float
		if colVal.Type() == storage.TypeInteger {
			var intVal int64
			intVal, ok = colVal.AsInt64()
			colFloat = float64(intVal)
		} else {
			colFloat, ok = colVal.AsFloat64()
		}

		if !ok {
			return false
		}

		// Convert expression value to float
		if e.ValueType == storage.TypeInteger {
			constFloat = float64(e.Int64Value)
		} else {
			constFloat = e.Float64Value
		}

		// Compare as floats
		switch e.Operator {
		case storage.EQ:
			return colFloat == constFloat
		case storage.NE:
			return colFloat != constFloat
		case storage.GT:
			return colFloat > constFloat
		case storage.GTE:
			return colFloat >= constFloat
		case storage.LT:
			return colFloat < constFloat
		case storage.LTE:
			return colFloat <= constFloat
		}
		return false
	}

	// Fall back to the standard evaluation for other types or mixed-type operations
	result, _ := e.Evaluate(row)
	return result
}

// GetColumnName returns the column name for this expression
func (e *FastSimpleExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the original expression value
func (e *FastSimpleExpression) GetValue() interface{} {
	switch e.ValueType {
	case storage.TypeInteger:
		return e.Int64Value
	case storage.TypeFloat:
		return e.Float64Value
	case storage.TypeString:
		return e.StringValue
	case storage.TypeBoolean:
		return e.BoolValue
	case storage.TypeTimestamp:
		return e.TimeValue
	default:
		return nil
	}
}

// GetOperator returns the operator for this expression
func (e *FastSimpleExpression) GetOperator() storage.Operator {
	return e.Operator
}

// CanUseIndex returns whether this expression can use an index
func (e *FastSimpleExpression) CanUseIndex() bool {
	return e.Operator == storage.EQ ||
		e.Operator == storage.GT ||
		e.Operator == storage.GTE ||
		e.Operator == storage.LT ||
		e.Operator == storage.LTE
}

// WithAliases implements the Expression interface
func (e *FastSimpleExpression) WithAliases(aliases map[string]string) storage.Expression {
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		return NewFastSimpleExpression(originalCol, e.Operator, e.GetValue())
	}
	return e
}

// FastPKDetector is a helper function to check if an expression is a primary key operation
// and convert it to a FastSimpleExpression for optimized execution
func FastPKDetector(expr storage.Expression, schema storage.Schema) (*FastSimpleExpression, bool) {
	// Fast exit if no expression
	if expr == nil {
		return nil, false
	}

	// Find the primary key column
	var pkColName string
	var pkColIndex int = -1

	for i, col := range schema.Columns {
		if col.PrimaryKey {
			pkColName = col.Name
			pkColIndex = i
			break
		}
	}

	// If no primary key found, can't optimize
	if pkColName == "" {
		return nil, false
	}

	// Unwrap SchemaAwareExpression if needed
	if schemaExpr, ok := expr.(*SchemaAwareExpression); ok {
		return FastPKDetector(schemaExpr.Expr, schema)
	}

	// Check if it's a SimpleExpression
	if simpleExpr, ok := expr.(*SimpleExpression); ok {
		// Only for primary key column
		if simpleExpr.Column != pkColName {
			return nil, false
		}

		// For supported operators
		if simpleExpr.Operator != storage.EQ &&
			simpleExpr.Operator != storage.NE &&
			simpleExpr.Operator != storage.GT &&
			simpleExpr.Operator != storage.GTE &&
			simpleExpr.Operator != storage.LT &&
			simpleExpr.Operator != storage.LTE {
			return nil, false
		}

		// Create optimized expression
		fastExpr := NewFastSimpleExpression(pkColName, simpleExpr.Operator, simpleExpr.Value)
		fastExpr.ColIndex = pkColIndex
		fastExpr.IndexPrepped = true

		return fastExpr, true
	}

	return nil, false
}
