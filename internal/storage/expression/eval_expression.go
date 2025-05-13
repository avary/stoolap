package expression

import (
	"fmt"

	"github.com/stoolap/stoolap/internal/storage"
)

// EvalExpression represents an expression implemented as a Go function
type EvalExpression struct {
	evalFn func(row storage.Row) (bool, error)

	// EvalExpression doesn't directly support aliases since it's
	// a black box function. However, the function inside could potentially
	// handle aliases if it's designed to do so.
}

// NewEvalExpression creates a new function expression using a function
func NewEvalExpression(evalFn func(row storage.Row) (bool, error)) *EvalExpression {
	return &EvalExpression{
		evalFn: evalFn,
	}
}

// Evaluate implements the storage.Expression interface
func (e *EvalExpression) Evaluate(row storage.Row) (bool, error) {
	// If evalFn is nil, always return false
	if e.evalFn == nil {
		return false, nil
	}

	fmt.Printf("Evaluating function expression with row: %v\n", row)
	return e.evalFn(row)
}

// GetColumnName returns an empty string since we don't know which column is used
func (e *EvalExpression) GetColumnName() string {
	return ""
}

// GetValue returns nil since we don't know what value is being compared
func (e *EvalExpression) GetValue() interface{} {
	return nil
}

// GetOperator returns an unknown operator
func (e *EvalExpression) GetOperator() storage.Operator {
	return storage.EQ // Arbitrary default
}

// CanUseIndex returns false since we can't optimize function expressions directly
func (e *EvalExpression) CanUseIndex() bool {
	return false
}

// WithAliases implements the Expression interface
// For EvalExpression, we can't modify the internal function, so we return a wrapper
// that applies alias handling around the original function
func (e *EvalExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a function wrapper that will attempt to convert alias names to real column names
	aliasedFn := func(row storage.Row) (bool, error) {
		// Just pass through to the original function for now
		// A more sophisticated implementation could try to intercept column lookups
		// but that would require more complex function composition
		return e.evalFn(row)
	}

	return &EvalExpression{
		evalFn: aliasedFn,
	}
}
