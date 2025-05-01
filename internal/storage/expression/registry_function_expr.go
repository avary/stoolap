package expression

import (
	"fmt"
	"strings"

	"github.com/semihalev/stoolap/internal/functions/registry"
	"github.com/semihalev/stoolap/internal/storage"
)

// RegistryFunctionExpression is an expression type that uses the function registry
// It represents SQL function calls like UPPER(column) = 'VALUE'
type RegistryFunctionExpression struct {
	functionName   string            // The function name from registry (e.g., "UPPER")
	columnName     string            // The column name to apply the function to
	value          interface{}       // The value to compare against
	operator       storage.Operator  // The comparison operator
	aliases        map[string]string // Column aliases
	originalColumn string            // Original column name if Column is an alias
}

// NewRegistryFunctionExpression creates a new function expression using the registry
func NewRegistryFunctionExpression(
	functionName string,
	columnName string,
	operator storage.Operator,
	value interface{}) storage.Expression {

	return &RegistryFunctionExpression{
		functionName: strings.ToUpper(functionName),
		columnName:   columnName,
		operator:     operator,
		value:        value,
	}
}

// Evaluate evaluates the function expression against a row
func (e *RegistryFunctionExpression) Evaluate(row storage.Row) (bool, error) {
	// Get global function registry
	funcRegistry := registry.GetGlobal()

	// Find the column value in the row
	var colValue interface{}
	var colFound bool

	for _, col := range row {
		if col.IsNull() {
			continue // NULL values can't be used with functions
		}

		// Get the column value based on type
		switch col.Type() {
		case storage.TEXT:
			if val, ok := col.AsString(); ok {
				colValue = val
				colFound = true
			}
		case storage.INTEGER:
			if val, ok := col.AsInt64(); ok {
				colValue = val
				colFound = true
			}
		case storage.FLOAT:
			if val, ok := col.AsFloat64(); ok {
				colValue = val
				colFound = true
			}
		case storage.BOOLEAN:
			if val, ok := col.AsBoolean(); ok {
				colValue = val
				colFound = true
			}
		case storage.TIMESTAMP, storage.DATE, storage.TIME:
			if val, ok := col.AsTimestamp(); ok {
				colValue = val
				colFound = true
			}
		}

		if colFound {
			break
		}
	}

	if !colFound || colValue == nil {
		return false, nil
	}

	// Get the function from the registry
	function := funcRegistry.GetScalarFunction(e.functionName)
	if function == nil {
		return false, nil
	}

	// Call the function with column value as argument
	result, err := function.Evaluate(colValue)
	if err != nil {
		return false, err
	}

	// Compare the function result with the value using the specified operator
	return compareWithOperator(result, e.value, e.operator)
}

// compareWithOperator compares two values using the specified operator
func compareWithOperator(left, right interface{}, operator storage.Operator) (bool, error) {
	switch operator {
	case storage.EQ:
		// Handle different type combinations for equality
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l == r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l == r, nil
			case int64:
				return int64(l) == r, nil
			case float64:
				return float64(l) == r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l == int64(r), nil
			case int64:
				return l == r, nil
			case float64:
				return float64(l) == r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l == float64(r), nil
			case int64:
				return l == float64(r), nil
			case float64:
				return l == r, nil
			}
		case bool:
			if r, ok := right.(bool); ok {
				return l == r, nil
			}
		}

	case storage.NE:
		// Handle different type combinations for inequality
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l != r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l != r, nil
			case int64:
				return int64(l) != r, nil
			case float64:
				return float64(l) != r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l != int64(r), nil
			case int64:
				return l != r, nil
			case float64:
				return float64(l) != r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l != float64(r), nil
			case int64:
				return l != float64(r), nil
			case float64:
				return l != r, nil
			}
		case bool:
			if r, ok := right.(bool); ok {
				return l != r, nil
			}
		}

	case storage.GT:
		// Handle different type combinations for greater than
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l > r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l > r, nil
			case int64:
				return int64(l) > r, nil
			case float64:
				return float64(l) > r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l > int64(r), nil
			case int64:
				return l > r, nil
			case float64:
				return float64(l) > r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l > float64(r), nil
			case int64:
				return l > float64(r), nil
			case float64:
				return l > r, nil
			}
		}

	case storage.GTE:
		// Handle different type combinations for greater than or equal
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l >= r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l >= r, nil
			case int64:
				return int64(l) >= r, nil
			case float64:
				return float64(l) >= r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l >= int64(r), nil
			case int64:
				return l >= r, nil
			case float64:
				return float64(l) >= r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l >= float64(r), nil
			case int64:
				return l >= float64(r), nil
			case float64:
				return l >= r, nil
			}
		}

	case storage.LT:
		// Handle different type combinations for less than
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l < r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l < r, nil
			case int64:
				return int64(l) < r, nil
			case float64:
				return float64(l) < r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l < int64(r), nil
			case int64:
				return l < r, nil
			case float64:
				return float64(l) < r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l < float64(r), nil
			case int64:
				return l < float64(r), nil
			case float64:
				return l < r, nil
			}
		}

	case storage.LTE:
		// Handle different type combinations for less than or equal
		switch l := left.(type) {
		case string:
			if r, ok := right.(string); ok {
				return l <= r, nil
			}
		case int:
			switch r := right.(type) {
			case int:
				return l <= r, nil
			case int64:
				return int64(l) <= r, nil
			case float64:
				return float64(l) <= r, nil
			}
		case int64:
			switch r := right.(type) {
			case int:
				return l <= int64(r), nil
			case int64:
				return l <= r, nil
			case float64:
				return float64(l) <= r, nil
			}
		case float64:
			switch r := right.(type) {
			case int:
				return l <= float64(r), nil
			case int64:
				return l <= float64(r), nil
			case float64:
				return l <= r, nil
			}
		}

	case storage.LIKE:
		// LIKE only works with strings
		if lStr, lOk := left.(string); lOk {
			if rStr, rOk := right.(string); rOk {
				return strings.Contains(lStr, rStr), nil
			}
		}
	}

	// Default comparison using string representation as a fallback
	return false, fmt.Errorf("can't compare %T with %T using %v", left, right, operator)
}

// GetColumnName returns the column name this expression operates on
func (e *RegistryFunctionExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *RegistryFunctionExpression) GetValue() interface{} {
	return e.value
}

// GetOperator returns the operator this expression uses
func (e *RegistryFunctionExpression) GetOperator() storage.Operator {
	return e.operator
}

// GetFunctionName returns the name of the function being applied
func (e *RegistryFunctionExpression) GetFunctionName() string {
	return e.functionName
}

// CanUseIndex returns true if this expression can use an index
func (e *RegistryFunctionExpression) CanUseIndex() bool {
	// For now, function expressions can't use indexes directly
	// In a full implementation, some functions could use functional indexes
	return false
}

// WithAliases implements the storage.Expression interface
func (e *RegistryFunctionExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &RegistryFunctionExpression{
		functionName: e.functionName,
		columnName:   e.columnName,
		operator:     e.operator,
		value:        e.value,
		aliases:      aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// Ensure RegistryFunctionExpression implements IndexableExpression
var _ IndexableExpression = (*RegistryFunctionExpression)(nil)
