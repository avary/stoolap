package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/stoolap/stoolap/internal/storage"
)

// InListExpression represents an expression that checks if a column value is in a list of values
type InListExpression struct {
	Column string
	Values []interface{}
	Not    bool // If true, this is a NOT IN expression

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// Make sure InListExpression implements IndexableExpression
var _ IndexableExpression = (*InListExpression)(nil)

// Evaluate implements the Expression interface
func (e *InListExpression) Evaluate(row storage.Row) (bool, error) {
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
		// NULL IN (...) is always false
		// For NOT IN this also remains false (NULL NOT IN ... is also false)
		// This is standard SQL behavior for NULL in comparisons
		return false, nil
	}

	// Flag to track if we found a match
	found := false

	// Try to use the appropriate type comparison based on column type
	switch colValue.Type() {
	case storage.TypeInteger:
		// For integers, convert and compare numerically
		colInt, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to int64 for comparison
			var valueInt int64
			switch v := value.(type) {
			case int:
				valueInt = int64(v)
			case int64:
				valueInt = v
			case float64:
				valueInt = int64(v)
			case string:
				// Try to parse as int
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
					valueInt = parsed
				} else {
					// Skip this value if not parseable
					continue
				}
			default:
				// Skip non-numeric values
				continue
			}

			// Compare
			if colInt == valueInt {
				found = true
				break // Found a match, no need to check further
			}
		}

	case storage.TypeFloat:
		// For floats, convert and compare numerically
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to float64 for comparison
			var valueFloat float64
			switch v := value.(type) {
			case int:
				valueFloat = float64(v)
			case int64:
				valueFloat = float64(v)
			case float64:
				valueFloat = v
			case string:
				// Try to parse as float
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					valueFloat = parsed
				} else {
					// Skip this value if not parseable
					continue
				}
			default:
				// Skip non-numeric values
				continue
			}

			// Compare
			if colFloat == valueFloat {
				found = true
				break // Found a match, no need to check further
			}
		}

	default:
		// For other types, fall back to string comparison
		colStrVal, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to string for comparison
			var valueStr string
			switch v := value.(type) {
			case string:
				valueStr = v
			default:
				valueStr = fmt.Sprintf("%v", v)
			}

			// Compare
			if colStrVal == valueStr {
				found = true
				break // Found a match, no need to check further
			}
		}
	}

	// For NOT IN, invert the result
	if e.Not {
		return !found, nil
	}

	return found, nil
}

// Implement the IndexableExpression interface

// GetColumnName returns the column name
func (e *InListExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the first value in the list (not very useful)
func (e *InListExpression) GetValue() interface{} {
	if len(e.Values) > 0 {
		return e.Values[0]
	}
	return nil
}

// GetOperator returns EQ (equals)
func (e *InListExpression) GetOperator() storage.Operator {
	return storage.EQ
}

// CanUseIndex returns true, as IN can potentially use an index
func (e *InListExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the Expression interface for InListExpression
func (e *InListExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &InListExpression{
		Column:  e.Column,
		Values:  e.Values,
		Not:     e.Not,
		aliases: aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}
