package expression

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// BetweenExpression represents an expression that checks if a column value is between a lower and upper bound
type BetweenExpression struct {
	Column     string
	LowerBound interface{}
	UpperBound interface{}
	Inclusive  bool

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// Make sure BetweenExpression implements IndexableExpression
var _ IndexableExpression = (*BetweenExpression)(nil)

// Evaluate implements the Expression interface for BetweenExpression
func (e *BetweenExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column in the row
	colIndex := -1
	for i, val := range row {
		if val != nil {
			if colVal, ok := val.(interface{ Name() string }); ok {
				colName := colVal.Name()
				// Match by column name - check both original and alias names
				if colName == e.Column || strings.EqualFold(colName, e.Column) {
					colIndex = i
					break
				}

				// Also check if we're looking for an alias but the row has original names
				if e.originalColumn != "" && (colName == e.originalColumn || strings.EqualFold(colName, e.originalColumn)) {
					colIndex = i
					break
				}
			}
		}
	}

	// Column not found in row
	if colIndex == -1 || colIndex >= len(row) {
		return false, nil
	}

	// Get the column value
	colValue := row[colIndex]
	if colValue == nil {
		// NULL BETWEEN ... is always false
		return false, nil
	}

	// Try to use the appropriate type comparison based on column type
	switch colValue.Type() {
	case storage.TypeInteger:
		// For integers, convert and compare numerically
		colInt, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Convert lower bound to int64
		var lowerInt int64
		switch v := e.LowerBound.(type) {
		case int:
			lowerInt = int64(v)
		case int64:
			lowerInt = v
		case float64:
			lowerInt = int64(v)
		case string:
			// Try to parse as int
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				lowerInt = parsed
			} else {
				return false, fmt.Errorf("cannot convert lower bound to int64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to int64
		var upperInt int64
		switch v := e.UpperBound.(type) {
		case int:
			upperInt = int64(v)
		case int64:
			upperInt = v
		case float64:
			upperInt = int64(v)
		case string:
			// Try to parse as int
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				upperInt = parsed
			} else {
				return false, fmt.Errorf("cannot convert upper bound to int64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colInt >= lowerInt && colInt <= upperInt, nil
		}
		return colInt > lowerInt && colInt < upperInt, nil

	case storage.TypeFloat:
		// For floats, convert and compare numerically
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Convert lower bound to float64
		var lowerFloat float64
		switch v := e.LowerBound.(type) {
		case int:
			lowerFloat = float64(v)
		case int64:
			lowerFloat = float64(v)
		case float64:
			lowerFloat = v
		case string:
			// Try to parse as float
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				lowerFloat = parsed
			} else {
				return false, fmt.Errorf("cannot convert lower bound to float64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to float64
		var upperFloat float64
		switch v := e.UpperBound.(type) {
		case int:
			upperFloat = float64(v)
		case int64:
			upperFloat = float64(v)
		case float64:
			upperFloat = v
		case string:
			// Try to parse as float
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				upperFloat = parsed
			} else {
				return false, fmt.Errorf("cannot convert upper bound to float64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colFloat >= lowerFloat && colFloat <= upperFloat, nil
		}
		return colFloat > lowerFloat && colFloat < upperFloat, nil

	case storage.TypeString:
		// For strings, compare lexicographically
		colStr, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Convert lower bound to string
		var lowerStr string
		switch v := e.LowerBound.(type) {
		case string:
			lowerStr = v
		default:
			lowerStr = fmt.Sprintf("%v", e.LowerBound)
		}

		// Convert upper bound to string
		var upperStr string
		switch v := e.UpperBound.(type) {
		case string:
			upperStr = v
		default:
			upperStr = fmt.Sprintf("%v", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colStr >= lowerStr && colStr <= upperStr, nil
		}
		return colStr > lowerStr && colStr < upperStr, nil

	case storage.TypeTimestamp:
		// For timestamp values, convert and compare
		colTime, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Convert lower bound to time.Time
		var lowerTime time.Time
		switch v := e.LowerBound.(type) {
		case time.Time:
			lowerTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTimestamp(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse lower bound as time: %s", v)
			}
			lowerTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to time.Time
		var upperTime time.Time
		switch v := e.UpperBound.(type) {
		case time.Time:
			upperTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTimestamp(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse upper bound as time: %s", v)
			}
			upperTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return (colTime.Equal(lowerTime) || colTime.After(lowerTime)) &&
				(colTime.Equal(upperTime) || colTime.Before(upperTime)), nil
		}
		return colTime.After(lowerTime) && colTime.Before(upperTime), nil

	case storage.TypeDate:
		// For date values, use AsDate instead of AsTimestamp
		colTime, ok := colValue.AsDate()
		if !ok {
			// Try AsTimestamp as fallback for backward compatibility
			colTime, ok = colValue.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		// Convert lower bound to time.Time
		var lowerTime time.Time
		switch v := e.LowerBound.(type) {
		case time.Time:
			lowerTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseDate(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse lower bound as time: %s", v)
			}
			lowerTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to time.Time
		var upperTime time.Time
		switch v := e.UpperBound.(type) {
		case time.Time:
			upperTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseDate(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse upper bound as time: %s", v)
			}
			upperTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return (colTime.Equal(lowerTime) || colTime.After(lowerTime)) &&
				(colTime.Equal(upperTime) || colTime.Before(upperTime)), nil
		}
		return colTime.After(lowerTime) && colTime.Before(upperTime), nil

	case storage.TypeTime:
		// First try to use AsTime for TIME values
		colTime, ok := colValue.AsTime()
		if !ok {
			// Fall back to AsTimestamp for backward compatibility
			colTime, ok = colValue.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		// Convert lower bound to time.Time
		var lowerTime time.Time
		switch v := e.LowerBound.(type) {
		case time.Time:
			lowerTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTime(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse lower bound as time: %s", v)
			}
			lowerTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to time.Time
		var upperTime time.Time
		switch v := e.UpperBound.(type) {
		case time.Time:
			upperTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTime(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse upper bound as time: %s", v)
			}
			upperTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return (colTime.Equal(lowerTime) || colTime.After(lowerTime)) &&
				(colTime.Equal(upperTime) || colTime.Before(upperTime)), nil
		}
		return colTime.After(lowerTime) && colTime.Before(upperTime), nil

	default:
		// For other types, just return false
		return false, fmt.Errorf("unsupported column type for BETWEEN: %v", colValue.Type())
	}
}

// GetColumnName implements IndexableExpression interface
func (e *BetweenExpression) GetColumnName() string {
	return e.Column
}

// GetValue implements IndexableExpression interface
// For BETWEEN expressions, we return the lower bound since that's typically used for index scanning
func (e *BetweenExpression) GetValue() interface{} {
	return e.LowerBound
}

// GetOperator implements IndexableExpression interface
// For BETWEEN expressions, we return GTE as the operator
// (since we'll start scanning from the lower bound)
func (e *BetweenExpression) GetOperator() storage.Operator {
	return storage.GTE
}

// CanUseIndex implements IndexableExpression interface
func (e *BetweenExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the Expression interface for BetweenExpression
func (e *BetweenExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &BetweenExpression{
		Column:     e.Column,
		LowerBound: e.LowerBound,
		UpperBound: e.UpperBound,
		Inclusive:  e.Inclusive,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}
