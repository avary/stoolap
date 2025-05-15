package expression

import (
	"fmt"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// SimpleExpression is a basic implementation of a boolean expression
type SimpleExpression struct {
	Column   string           // Name of the column
	Operator storage.Operator // Comparison operator
	Value    interface{}      // Value to compare against (for backward compatibility)

	// Optimized typed value storage to avoid interface{} overhead
	Int64Value   int64
	Float64Value float64
	StringValue  string
	BoolValue    bool
	TimeValue    time.Time

	// Type information and optimization flags
	ValueType    storage.DataType // Type of the value for fast paths
	ValueIsNil   bool             // Whether the value is nil
	ColIndex     int              // Column index for fast lookup
	IndexPrepped bool             // Whether we've prepared column index mapping

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewSimpleExpression creates a new SimpleExpression
func NewSimpleExpression(column string, operator storage.Operator, value interface{}) *SimpleExpression {
	expr := &SimpleExpression{
		Column:       column,
		Operator:     operator,
		Value:        value,
		IndexPrepped: false,
		ColIndex:     -1,
		ValueIsNil:   value == nil,
		aliases:      make(map[string]string),
	}

	// Pre-process the value to avoid repeated type assertions during evaluation
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
			expr.StringValue = fmt.Sprintf("%v", value)
		}
	}

	return expr
}

// Evaluate implements the Expression interface
func (e *SimpleExpression) Evaluate(row storage.Row) (bool, error) {
	// Quick validations
	if len(row) == 0 {
		return false, nil
	}

	// Use cached column index if prepared
	if e.IndexPrepped && e.ColIndex >= 0 && e.ColIndex < len(row) {
		colVal := row[e.ColIndex]
		if colVal == nil {
			return e.evaluateNull()
		}
		if colVal.IsNull() {
			return e.evaluateNull()
		}
		return e.evaluateComparison(colVal)
	}

	// If column index not prepared, simply return false
	// We don't want to do expensive column name lookups or interface calls
	return false, nil
}

// IsEquality returns true if this is an equality expression
func (e *SimpleExpression) IsEquality() bool {
	return e.Operator == storage.EQ
}

// GetColumnName returns the column name for this expression
func (e *SimpleExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the value for this expression
func (e *SimpleExpression) GetValue() interface{} {
	return e.Value
}

// GetOperator returns the operator for this expression
func (e *SimpleExpression) GetOperator() storage.Operator {
	return storage.Operator(e.Operator)
}

// CanUseIndex returns whether this expression can use an index
func (e *SimpleExpression) CanUseIndex() bool {
	// Allow index usage for comparison operators as well
	return e.Operator == storage.EQ ||
		e.Operator == storage.GT ||
		e.Operator == storage.GTE ||
		e.Operator == storage.LT ||
		e.Operator == storage.LTE
}

// WithAliases implements the Expression interface for SimpleExpression
func (e *SimpleExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &SimpleExpression{
		Column:       e.Column,
		Operator:     e.Operator,
		Value:        e.Value,
		Int64Value:   e.Int64Value,
		Float64Value: e.Float64Value,
		StringValue:  e.StringValue,
		BoolValue:    e.BoolValue,
		TimeValue:    e.TimeValue,
		ValueType:    e.ValueType,
		ValueIsNil:   e.ValueIsNil,
		IndexPrepped: false, // Reset index preparation as column names might change
		ColIndex:     -1,    // Reset column index
		aliases:      aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}

// SetOriginalColumn sets the original column name for this expression
func (e *SimpleExpression) SetOriginalColumn(col string) {
	e.originalColumn = col
}

// evaluateNull handles comparisons against NULL values
func (e *SimpleExpression) evaluateNull() (bool, error) {
	switch e.Operator {
	case storage.ISNULL:
		return true, nil
	case storage.ISNOTNULL:
		return false, nil
	default:
		// NULL comparisons with other operators return false
		return false, nil
	}
}

// evaluateComparison evaluates a comparison between a column value and expression value
func (e *SimpleExpression) evaluateComparison(colValue storage.ColumnValue) (bool, error) {
	// Fast path for direct type comparisons - most common case
	if e.ValueType == colValue.Type() {
		return e.evaluateTypedComparison(colValue)
	}

	// Fast path for numeric types (INTEGER vs FLOAT)
	if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
		(colValue.Type() == storage.TypeInteger || colValue.Type() == storage.TypeFloat) {
		return e.evaluateNumericComparison(colValue)
	}

	// Fall back to original implementation for more complex type conversions
	switch colValue.Type() {
	case storage.INTEGER:
		v, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value when possible
		var compareValue int64
		if e.ValueType == storage.TypeInteger {
			compareValue = e.Int64Value
		} else {
			// Use the legacy path for other types
			switch val := e.Value.(type) {
			case int:
				compareValue = int64(val)
			case int64:
				compareValue = val
			case float64:
				compareValue = int64(val)
			default:
				// Special handling for mvcc.IntegerValue and other custom types
				if intVal, ok := e.Value.(interface{ AsInt64() (int64, bool) }); ok {
					if i64, ok := intVal.AsInt64(); ok {
						compareValue = i64
					} else {
						return false, fmt.Errorf("failed to convert %T to int64", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare integer with %T", e.Value)
				}
			}
		}

		// Apply the operator
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", e.Operator)
		}

	case storage.FLOAT:
		v, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value when possible
		var compareValue float64
		if e.ValueType == storage.TypeFloat {
			compareValue = e.Float64Value
		} else if e.ValueType == storage.TypeInteger {
			compareValue = float64(e.Int64Value)
		} else {
			// Use the legacy path for other types
			switch val := e.Value.(type) {
			case int:
				compareValue = float64(val)
			case int64:
				compareValue = float64(val)
			case float64:
				compareValue = val
			default:
				// Special handling for mvcc.FloatValue and other custom types
				if floatVal, ok := e.Value.(interface{ AsFloat64() (float64, bool) }); ok {
					if f64, ok := floatVal.AsFloat64(); ok {
						compareValue = f64
					} else {
						return false, fmt.Errorf("failed to convert %T to float64", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare float with %T", e.Value)
				}
			}
		}

		// Apply the operator
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for float: %v", e.Operator)
		}

	case storage.TEXT:
		v, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value when possible
		var compareValue string
		if e.ValueType == storage.TypeString {
			compareValue = e.StringValue
		} else {
			// Use the legacy path for other types
			switch val := e.Value.(type) {
			case string:
				compareValue = val
			default:
				// Special handling for mvcc.StringValue and other custom types
				if strVal, ok := e.Value.(interface{ AsString() (string, bool) }); ok {
					if str, ok := strVal.AsString(); ok {
						compareValue = str
					} else {
						compareValue = fmt.Sprintf("%v", e.Value)
					}
				} else {
					compareValue = fmt.Sprintf("%v", val)
				}
			}
		}

		// Apply the operator
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		case storage.LIKE:
			// LIKE pattern matching would need more implementation
			return false, fmt.Errorf("LIKE operator not implemented yet")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", e.Operator)
		}

	case storage.BOOLEAN:
		v, ok := colValue.AsBoolean()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value when possible
		var compareValue bool
		if e.ValueType == storage.TypeBoolean {
			compareValue = e.BoolValue
		} else {
			// Use the legacy path for other types
			switch val := e.Value.(type) {
			case bool:
				compareValue = val
			case string:
				compareValue = val == "true" || val == "1"
			case int:
				compareValue = val != 0
			case int64:
				compareValue = val != 0
			default:
				// Special handling for mvcc.BooleanValue and other custom types
				if boolVal, ok := e.Value.(interface{ AsBoolean() (bool, bool) }); ok {
					if b, ok := boolVal.AsBoolean(); ok {
						compareValue = b
					} else {
						return false, fmt.Errorf("failed to convert %T to boolean", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare boolean with %T", e.Value)
				}
			}
		}

		// Apply the operator
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", e.Operator)
		}

	case storage.TIMESTAMP:
		// For timestamps, use a similar approach to date but with full precision
		timestamp, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value when possible
		var compareTime time.Time
		if e.ValueType == storage.TypeTimestamp {
			compareTime = e.TimeValue
		} else {
			// Use the legacy path for other types
			switch val := e.Value.(type) {
			case time.Time:
				compareTime = val
			case string:
				var err error
				compareTime, err = storage.ParseTimestamp(val)
				if err != nil {
					return false, fmt.Errorf("could not parse timestamp string: %v", err)
				}
			default:
				return false, fmt.Errorf("cannot compare timestamp with %T", e.Value)
			}
		}

		// Apply the operator to the timestamps
		switch e.Operator {
		case storage.EQ:
			return timestamp.Equal(compareTime), nil
		case storage.NE:
			return !timestamp.Equal(compareTime), nil
		case storage.GT:
			return timestamp.After(compareTime), nil
		case storage.GTE:
			return timestamp.After(compareTime) || timestamp.Equal(compareTime), nil
		case storage.LT:
			return timestamp.Before(compareTime), nil
		case storage.LTE:
			return timestamp.Before(compareTime) || timestamp.Equal(compareTime), nil
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", e.Operator)
		}
	}

	// Default case - type not supported for comparison
	return false, fmt.Errorf("unsupported column type for comparison: %v", colValue.Type())
}

// evaluateTypedComparison provides a fast path for comparing values of the same type
func (e *SimpleExpression) evaluateTypedComparison(colValue storage.ColumnValue) (bool, error) {
	switch e.ValueType {
	case storage.TypeInteger:
		v, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

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
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", e.Operator)
		}

	case storage.TypeFloat:
		v, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

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
		default:
			return false, fmt.Errorf("unsupported operator for float: %v", e.Operator)
		}

	case storage.TypeString:
		v, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

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
		case storage.LIKE:
			// LIKE pattern matching would need more implementation
			return false, fmt.Errorf("LIKE operator not implemented yet")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", e.Operator)
		}

	case storage.TypeBoolean:
		v, ok := colValue.AsBoolean()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.BoolValue, nil
		case storage.NE:
			return v != e.BoolValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", e.Operator)
		}

	case storage.TypeTimestamp:
		v, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

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
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", e.Operator)
		}
	}

	return false, fmt.Errorf("unsupported value type: %v", e.ValueType)
}

// evaluateNumericComparison provides a fast path for numeric comparisons between integer and float types
func (e *SimpleExpression) evaluateNumericComparison(colValue storage.ColumnValue) (bool, error) {
	// Convert both values to float64 for comparison
	var colFloat float64

	// Get column value as float
	if colValue.Type() == storage.TypeInteger {
		intVal, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}
		colFloat = float64(intVal)
	} else {
		var ok bool
		colFloat, ok = colValue.AsFloat64()
		if !ok {
			return false, nil
		}
	}

	// Get expression value as float
	var exprFloat float64
	if e.ValueType == storage.TypeInteger {
		exprFloat = float64(e.Int64Value)
	} else {
		exprFloat = e.Float64Value
	}

	// Perform the comparison
	switch e.Operator {
	case storage.EQ:
		return colFloat == exprFloat, nil
	case storage.NE:
		return colFloat != exprFloat, nil
	case storage.GT:
		return colFloat > exprFloat, nil
	case storage.GTE:
		return colFloat >= exprFloat, nil
	case storage.LT:
		return colFloat < exprFloat, nil
	case storage.LTE:
		return colFloat <= exprFloat, nil
	default:
		return false, fmt.Errorf("unsupported operator for numeric comparison: %v", e.Operator)
	}
}

// PrepareForSchema optimizes the expression for a specific schema by calculating
// column indices in advance for fast evaluation
func (e *SimpleExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already prepped with valid index, don't redo the work
	if e.IndexPrepped && e.ColIndex >= 0 {
		return e
	}

	// Find the column index to optimize lookup
	for i, col := range schema.Columns {
		if col.Name == e.Column {
			e.ColIndex = i
			break
		} else if strings.EqualFold(col.Name, e.Column) {
			// Try case-insensitive match as a fallback
			e.ColIndex = i
			break
		}
	}

	e.IndexPrepped = true
	return e
}

// EvaluateFast is an ultra-optimized version of Evaluate that avoids interface method calls
// and type assertions where possible, for the critical path in query processing
func (e *SimpleExpression) EvaluateFast(row storage.Row) bool {
	// Use cached column index if available
	colIdx := e.ColIndex
	if !e.IndexPrepped || colIdx < 0 || colIdx >= len(row) {
		// If column index not prepared, simply return false
		// We don't want to do expensive column name lookups or interface calls
		return false
	}

	// Get the column value directly - inlined for speed
	colVal := row[colIdx]

	// Handle NULL cases first (very common case)
	if colVal == nil {
		return e.Operator == storage.ISNULL
	}

	// NULL check operations
	if e.Operator == storage.ISNULL {
		return colVal.IsNull()
	}

	if e.Operator == storage.ISNOTNULL {
		return !colVal.IsNull()
	}

	// For all other operations, NULL values always yield false
	if colVal.IsNull() {
		return false
	}

	// Fast path for integer comparison (most common case)
	if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
		v, ok := colVal.AsInt64()
		if !ok {
			return false
		}

		// Inline comparison for maximum speed
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
		default:
			return false
		}
	}

	// Fast path for float comparison
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
		default:
			return false
		}
	}

	// Fast path for string comparison
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
		default:
			return false
		}
	}

	// Fast path for boolean comparison
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
		default:
			return false
		}
	}

	// Fast path for timestamp comparison
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
		default:
			return false
		}
	}

	// Special optimized path for numeric type conversions (integer <-> float)
	if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
		(colVal.Type() == storage.TypeInteger || colVal.Type() == storage.TypeFloat) {

		// Extract column value as float
		var colFloat float64
		if colVal.Type() == storage.TypeInteger {
			intVal, ok := colVal.AsInt64()
			if !ok {
				return false
			}
			colFloat = float64(intVal)
		} else {
			var ok bool
			colFloat, ok = colVal.AsFloat64()
			if !ok {
				return false
			}
		}

		// Extract expression value as float
		var exprFloat float64
		if e.ValueType == storage.TypeInteger {
			exprFloat = float64(e.Int64Value)
		} else {
			exprFloat = e.Float64Value
		}

		// Compare directly
		switch e.Operator {
		case storage.EQ:
			return colFloat == exprFloat
		case storage.NE:
			return colFloat != exprFloat
		case storage.GT:
			return colFloat > exprFloat
		case storage.GTE:
			return colFloat >= exprFloat
		case storage.LT:
			return colFloat < exprFloat
		case storage.LTE:
			return colFloat <= exprFloat
		default:
			return false
		}
	}

	// Fallback to standard path for more complex cases
	result, _ := e.evaluateComparison(colVal)
	return result
}

// PrimaryKeyDetector is a helper function to check if an expression is a primary key operation
func PrimaryKeyDetector(expr storage.Expression, schema storage.Schema) (*SimpleExpression, bool) {
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

		simpleExpr.ColIndex = pkColIndex
		simpleExpr.IndexPrepped = true

		return simpleExpr, true
	}

	return nil, false
}
