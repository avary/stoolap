package expression

import (
	"github.com/stoolap/stoolap/internal/storage"
)

// NotExpression represents a logical NOT of an expression
type NotExpression struct {
	Expr storage.Expression
}

// NewNotExpression creates a new NOT expression
func NewNotExpression(expr storage.Expression) *NotExpression {
	return &NotExpression{
		Expr: expr,
	}
}

// Evaluate implements the Expression interface for NOT expressions
func (e *NotExpression) Evaluate(row storage.Row) (bool, error) {
	result, err := e.Expr.Evaluate(row)
	if err != nil {
		return false, err
	}
	return !result, nil
}

// WithAliases implements the Expression interface
func (e *NotExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Apply aliases to the inner expression
	if aliasable, ok := e.Expr.(interface {
		WithAliases(map[string]string) storage.Expression
	}); ok {
		// Create a new expression with the aliased inner expression
		return &NotExpression{
			Expr: aliasable.WithAliases(aliases),
		}
	}

	// If inner expression doesn't support aliases, return a copy of this expression
	return &NotExpression{
		Expr: e.Expr,
	}
}
