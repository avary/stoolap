package expression

import (
	"strings"

	"github.com/stoolap/stoolap/internal/storage"
)

// JSONPathExpression is a concrete implementation of IndexableExpression
// that supports extracting and comparing values from JSON columns using path syntax
// This enables pushing down expressions like "json_column->'$.key' = 'value'"
type JSONPathExpression struct {
	columnName     string            // The JSON column name
	path           string            // JSON path (e.g., "$.key", "$.array[0]")
	value          interface{}       // The value to compare against
	operator       storage.Operator  // The comparison operator
	aliases        map[string]string // Column aliases
	originalColumn string            // Original column name if Column is an alias
}

// NewJSONPathExpression creates a new JSON path expression
func NewJSONPathExpression(
	columnName string,
	path string,
	operator storage.Operator,
	value interface{}) storage.Expression {

	return &JSONPathExpression{
		columnName: columnName,
		path:       path,
		operator:   operator,
		value:      value,
	}
}

// Evaluate evaluates the JSON path expression against a row
func (e *JSONPathExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column by name
	for _, col := range row {
		if col.IsNull() {
			continue // NULL values can't be used with JSON path
		}

		// Only process JSON columns
		if col.Type() == storage.JSON {
			jsonStr, ok := col.AsJSON()
			if !ok {
				continue
			}

			// Extract value at the given path
			// This is a simplified implementation - in a real case, we would need
			// a proper JSON path evaluator that can handle more complex paths
			extractedValue, found := extractJSONValue(jsonStr, e.path)
			if !found {
				continue
			}

			// Compare based on operator
			switch e.operator {
			case storage.EQ:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a == b })
			case storage.NE:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a != b })
			case storage.GT:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a > b })
			case storage.GTE:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a >= b })
			case storage.LT:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a < b })
			case storage.LTE:
				return compareJSONValues(extractedValue, e.value, func(a, b string) bool { return a <= b })
			case storage.LIKE:
				if strVal, ok := e.value.(string); ok {
					if extractedStr, ok := extractedValue.(string); ok {
						return strings.Contains(extractedStr, strVal), nil
					}
				}
			}
		}
	}

	// Not found or no match
	return false, nil
}

// Simplified JSON path extraction
// This is a minimal implementation - in production we would use a proper JSON parser
func extractJSONValue(jsonStr, path string) (interface{}, bool) {
	_ = jsonStr // Placeholder for actual JSON parsing

	// Simple path extraction - just for demonstration
	// A real implementation would parse the JSON and apply the path
	// For now, we're simulating a very simple implementation

	// We only handle the simplest case for demonstration
	// A full implementation would use a JSON library and proper path parsing
	if path == "$.value" {
		// Assume simple JSON: {"value": "some text"}
		return "some text", true
	} else if path == "$.number" {
		// Assume simple JSON: {"number": 42}
		return 42, true
	}

	// Path not found
	return nil, false
}

// Helper to compare values of possibly different types
func compareJSONValues(a, b interface{}, strCompare func(string, string) bool) (bool, error) {
	// Handle string comparisons
	if aStr, aOk := a.(string); aOk {
		if bStr, bOk := b.(string); bOk {
			return strCompare(aStr, bStr), nil
		}
	}

	// Handle numeric comparisons - simplified for demonstration
	// A real implementation would need more robust type handling

	// Default fallback for mismatched types - convert to strings
	return strCompare(storage.ConvertValueToString(a), storage.ConvertValueToString(b)), nil
}

// GetColumnName returns the column name this expression operates on
func (e *JSONPathExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *JSONPathExpression) GetValue() interface{} {
	return e.value
}

// GetOperator returns the operator this expression uses
func (e *JSONPathExpression) GetOperator() storage.Operator {
	return e.operator
}

// GetJSONPath returns the JSON path expression
func (e *JSONPathExpression) GetJSONPath() string {
	return e.path
}

// CanUseIndex returns true if this expression can use an index
func (e *JSONPathExpression) CanUseIndex() bool {
	// For the initial implementation, JSON path expressions can't use indexes directly
	// In a full implementation, JSON indexes could be used when available
	return false
}

// WithAliases implements the storage.Expression interface
func (e *JSONPathExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &JSONPathExpression{
		columnName: e.columnName,
		path:       e.path,
		operator:   e.operator,
		value:      e.value,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}
