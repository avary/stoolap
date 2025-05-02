package expression

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// Make sure FunctionExpression implements IndexableExpression interface
var _ IndexableExpression = (*FunctionExpression)(nil)

// FunctionExpression represents an expression implemented as a Go function
type FunctionExpression struct {
	evalFn func(row storage.Row) (bool, error)

	// FunctionExpression doesn't directly support aliases since it's
	// a black box function. However, the function inside could potentially
	// handle aliases if it's designed to do so.
}

// NewSimpleExpression creates a new simple expression using a function
func NewSimpleExpression(evalFn func(row storage.Row) (bool, error)) *FunctionExpression {
	return &FunctionExpression{
		evalFn: evalFn,
	}
}

// Evaluate implements the storage.Expression interface
func (e *FunctionExpression) Evaluate(row storage.Row) (bool, error) {
	// If evalFn is nil, always return false
	if e.evalFn == nil {
		return false, nil
	}

	return e.evalFn(row)
}

// Implement IndexableExpression interface for FunctionExpression
// This doesn't actually enable index usage since we don't have that metadata,
// but allows the optimizer to handle FunctionExpressions without type assertion errors

// GetColumnName returns an empty string since we don't know which column is used
func (e *FunctionExpression) GetColumnName() string {
	return ""
}

// GetValue returns nil since we don't know what value is being compared
func (e *FunctionExpression) GetValue() interface{} {
	return nil
}

// GetOperator returns an unknown operator
func (e *FunctionExpression) GetOperator() storage.Operator {
	return storage.EQ // Arbitrary default
}

// CanUseIndex returns false since we can't optimize function expressions directly
func (e *FunctionExpression) CanUseIndex() bool {
	return false
}

// WithAliases implements the Expression interface
// For FunctionExpression, we can't modify the internal function, so we return a wrapper
// that applies alias handling around the original function
func (e *FunctionExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a function wrapper that will attempt to convert alias names to real column names
	aliasedFn := func(row storage.Row) (bool, error) {
		// Just pass through to the original function for now
		// A more sophisticated implementation could try to intercept column lookups
		// but that would require more complex function composition
		return e.evalFn(row)
	}

	return &FunctionExpression{
		evalFn: aliasedFn,
	}
}

// SimpleExpression is a basic implementation of a boolean expression
type SimpleExpression struct {
	Column   string           // Name of the column
	Operator storage.Operator // Comparison operator
	Value    interface{}      // Value to compare against

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// Evaluate implements the Expression interface
func (e *SimpleExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column index
	colIdx := -1
	for i, val := range row {
		if val != nil {
			// Check if the column implements the Name() method
			if colVal, ok := val.(interface{ Name() string }); ok {
				colName := colVal.Name()
				// Match by column name - check both original and alias names
				if colName == e.Column || strings.EqualFold(colName, e.Column) {
					colIdx = i
					break
				}

				// Also check if we're looking for an alias but the row has original names
				if e.originalColumn != "" && (colName == e.originalColumn || strings.EqualFold(colName, e.originalColumn)) {
					colIdx = i
					break
				}
			}
		}
	}

	// If not found, try a simpler approach
	if colIdx == -1 {
		for i, val := range row {
			if val != nil {
				// We don't have a way to know the column name directly from some row implementations
				// so we'd need to pass in the schema or do lookup elsewhere
				// For now, we'll just use the first non-nil column as a fallback
				colIdx = i
				break
			}
		}
	}

	if colIdx == -1 || colIdx >= len(row) {
		return false, fmt.Errorf("column %s not found in row", e.Column)
	}

	// Get the column value
	colVal := row[colIdx]
	if colVal == nil {
		// Special handling for NULL values
		return e.evaluateNull(colVal)
	}
	// Check if the column value is of the expected type
	return e.compareValues(colVal)
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
		Column:   e.Column,
		Operator: e.Operator,
		Value:    e.Value,
		aliases:  aliases,
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

// evaluateNull handles comparing against NULL values
func (e *SimpleExpression) evaluateNull(colVal storage.ColumnValue) (bool, error) {
	// First, check if the column value is actually NULL
	isNull := colVal == nil || colVal.IsNull()

	// Then evaluate based on the operator
	switch e.Operator {
	case storage.ISNULL:
		// For IS NULL, return true if the value is NULL
		return isNull, nil
	case storage.ISNOTNULL:
		// For IS NOT NULL, return true if the value is NOT NULL
		return !isNull, nil
	default:
		// NULL comparisons with other operators return false
		return false, nil
	}
}

// compareValues compares the column value with the expression value
func (e *SimpleExpression) compareValues(colVal storage.ColumnValue) (bool, error) {
	switch colVal.Type() {
	case storage.INTEGER:
		v, ok := colVal.AsInt64()
		if !ok {
			return false, nil
		}

		// Convert value to int64 for comparison
		var compareValue int64
		switch value := e.Value.(type) {
		case int:
			compareValue = int64(value)
		case int64:
			compareValue = value
		case float64:
			compareValue = int64(value)
		case string:
			// Try to parse string as number
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return false, fmt.Errorf("cannot compare integer with string: %s", value)
			}
			compareValue = parsed
		default:
			return false, fmt.Errorf("unsupported value type for integer comparison: %T", value)
		}

		// Apply the operator
		switch e.Operator {
		case storage.EQ:
			result := v == compareValue
			return result, nil
		case storage.NE:
			result := v != compareValue
			return result, nil
		case storage.GT:
			result := v > compareValue
			return result, nil
		case storage.GTE:
			result := v >= compareValue
			return result, nil
		case storage.LT:
			result := v < compareValue
			return result, nil
		case storage.LTE:
			result := v <= compareValue
			return result, nil
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", e.Operator)
		}

	case storage.FLOAT:
		v, ok := colVal.AsFloat64()
		if !ok {
			return false, nil
		}

		// Convert value to float64 for comparison
		var compareValue float64
		switch value := e.Value.(type) {
		case int:
			compareValue = float64(value)
		case int64:
			compareValue = float64(value)
		case float64:
			compareValue = value
		case string:
			// Try to parse string as number
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return false, fmt.Errorf("cannot compare float with string: %s", value)
			}
			compareValue = parsed
		default:
			return false, fmt.Errorf("unsupported value type for float comparison: %T", value)
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
		v, ok := colVal.AsString()
		if !ok {
			return false, nil
		}

		// Convert value to string for comparison
		var compareValue string
		switch value := e.Value.(type) {
		case string:
			compareValue = value
		case int, int64, float64, bool:
			compareValue = fmt.Sprintf("%v", value)
		default:
			return false, fmt.Errorf("unsupported value type for string comparison: %T", value)
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
			// LIKE operator would need implementation of pattern matching
			return false, fmt.Errorf("LIKE operator not implemented for string")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", e.Operator)
		}

	case storage.BOOLEAN:
		v, ok := colVal.AsBoolean()
		if !ok {
			return false, nil
		}

		// For boolean, only equality and inequality make sense
		compareValue, ok := e.Value.(bool)
		if !ok {
			// Try to convert from string
			if strValue, isStr := e.Value.(string); isStr {
				compareValue = strValue == "true" || strValue == "1"
				ok = true
			}

			if !ok {
				return false, fmt.Errorf("unsupported value type for boolean comparison: %T", e.Value)
			}
		}

		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", e.Operator)
		}

	case storage.TIMESTAMP:
		// Timestamp comparisons
		v, ok := colVal.AsTimestamp()
		if !ok {
			return false, nil
		}

		var compareValue time.Time
		switch value := e.Value.(type) {
		case time.Time:
			compareValue = value
		case string:
			// Try to parse as time
			parsed, err := storage.ParseTimestamp(value)
			if err != nil {
				return false, fmt.Errorf("cannot parse timestamp string: %s", value)
			}
			compareValue = parsed
		default:
			return false, fmt.Errorf("unsupported value type for timestamp comparison: %T", value)
		}

		switch e.Operator {
		case storage.EQ:
			return v.Equal(compareValue), nil
		case storage.NE:
			return !v.Equal(compareValue), nil
		case storage.GT:
			return v.After(compareValue), nil
		case storage.GTE:
			return v.After(compareValue) || v.Equal(compareValue), nil
		case storage.LT:
			return v.Before(compareValue), nil
		case storage.LTE:
			return v.Before(compareValue) || v.Equal(compareValue), nil
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", e.Operator)
		}

	case storage.DATE:
		// Date comparisons - use AsDate instead of AsTimestamp
		v, ok := colVal.AsDate()
		if !ok {
			// Fallback to AsTimestamp for backwards compatibility
			v, ok = colVal.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		var compareValue time.Time
		switch value := e.Value.(type) {
		case time.Time:
			compareValue = value
		case string:
			// Try multiple date formats to support various inputs
			parsed, err := storage.ParseDate(value)
			if err != nil {
				return false, fmt.Errorf("cannot parse date string: %s", value)
			}
			compareValue = parsed
		default:
			return false, fmt.Errorf("unsupported value type for date comparison: %T", value)
		}

		// Normalize dates for proper comparison
		// For date comparison, we need to handle the time component consistently

		// Normalize row value to start of day
		normalizedV := storage.NormalizeDate(v)

		// Set comparison value based on operator
		var normalizedCV time.Time

		if e.Operator == storage.LTE {
			// For <= operators with dates, use end of day (23:59:59.999999999)
			// This ensures '2023-07-01' includes the entire day
			normalizedCV = storage.DateToEndOfDay(compareValue)
		} else {
			// For all other operators, normalize to start of day
			normalizedCV = storage.NormalizeDate(compareValue)
		}

		// Use time.Time's built-in comparison methods on normalized values
		switch e.Operator {
		case storage.EQ:
			return normalizedV.Equal(normalizedCV), nil
		case storage.NE:
			return !normalizedV.Equal(normalizedCV), nil
		case storage.GT:
			return normalizedV.After(normalizedCV), nil
		case storage.GTE:
			return normalizedV.After(normalizedCV) || normalizedV.Equal(normalizedCV), nil
		case storage.LT:
			return normalizedV.Before(normalizedCV), nil
		case storage.LTE:
			return normalizedV.Before(normalizedCV) || normalizedV.Equal(normalizedCV), nil
		default:
			return false, fmt.Errorf("unsupported operator for date: %v", e.Operator)
		}

	case storage.TIME:
		// Time comparisons (only comparing the time part)
		// First try to use AsTime for TIME values
		v, ok := colVal.AsTime()
		if !ok {
			// Fall back to AsTimestamp for backward compatibility
			v, ok = colVal.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		var compareValue time.Time
		switch value := e.Value.(type) {
		case time.Time:
			compareValue = value
		case string:
			parsed, err := storage.ParseTime(value)
			if err != nil {
				return false, fmt.Errorf("cannot parse time string: %s", value)
			}
			compareValue = parsed
		default:
			return false, fmt.Errorf("unsupported value type for time comparison: %T", value)
		}

		// Ensure both times have the same year, month, day for consistent comparison
		vNorm := storage.NormalizeTime(v)
		compareNorm := storage.NormalizeTime(compareValue)

		// Compare time components as a whole using normalized times
		switch e.Operator {
		case storage.EQ:
			return vNorm.Equal(compareNorm), nil
		case storage.NE:
			return !vNorm.Equal(compareNorm), nil
		case storage.GT:
			return vNorm.After(compareNorm), nil
		case storage.GTE:
			return vNorm.After(compareNorm) || vNorm.Equal(compareNorm), nil
		case storage.LT:
			return vNorm.Before(compareNorm), nil
		case storage.LTE:
			return vNorm.Before(compareNorm) || vNorm.Equal(compareNorm), nil
		default:
			return false, fmt.Errorf("unsupported operator for time: %v", e.Operator)
		}

	case storage.JSON:
		// For JSON, we only support equality for now
		v, ok := colVal.AsJSON()
		if !ok {
			return false, nil
		}

		switch value := e.Value.(type) {
		case string:
			if e.Operator == storage.EQ {
				return v == value, nil
			} else if e.Operator == storage.NE {
				return v != value, nil
			}
		}
		return false, fmt.Errorf("unsupported operator for JSON: %v", e.Operator)

	default:
		return false, fmt.Errorf("unsupported column type for comparison: %v", colVal.Type())
	}
}
