package expression

import (
	"github.com/stoolap/stoolap/internal/storage"
)

// AndExpression represents a logical AND of multiple expressions
type AndExpression struct {
	Expressions             []storage.Expression
	aliases                 map[string]string    // Column aliases
	cachedSchemaExpressions []storage.Expression // Cached schema-aware expressions for performance
}

// NewAndExpression creates a new AND expression
func NewAndExpression(expressions ...storage.Expression) *AndExpression {
	return &AndExpression{
		Expressions: expressions,
	}
}

// Evaluate implements the Expression interface for AND expressions
func (e *AndExpression) Evaluate(row storage.Row) (bool, error) {
	for _, expr := range e.Expressions {
		result, err := expr.Evaluate(row)
		if err != nil {
			return false, err
		}
		if !result {
			return false, nil // Short-circuit on first false
		}
	}
	return true, nil // All expressions were true
}

// WithAliases implements the Expression interface
func (e *AndExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same structure
	result := &AndExpression{
		Expressions: make([]storage.Expression, len(e.Expressions)),
		aliases:     aliases,
	}

	// Apply aliases to all child expressions
	for i, expr := range e.Expressions {
		if aliasable, ok := expr.(interface {
			WithAliases(map[string]string) storage.Expression
		}); ok {
			result.Expressions[i] = aliasable.WithAliases(aliases)
		} else {
			result.Expressions[i] = expr
		}
	}

	return result
}

// OrExpression represents a logical OR of multiple expressions
type OrExpression struct {
	Expressions             []storage.Expression
	aliases                 map[string]string    // Column aliases
	cachedSchemaExpressions []storage.Expression // Cached schema-aware expressions for performance
}

// NewOrExpression creates a new OR expression
func NewOrExpression(expressions ...storage.Expression) *OrExpression {
	return &OrExpression{
		Expressions: expressions,
	}
}

// Evaluate implements the Expression interface for OR expressions
func (e *OrExpression) Evaluate(row storage.Row) (bool, error) {
	for _, expr := range e.Expressions {
		result, err := expr.Evaluate(row)
		if err != nil {
			continue // Skip errors in OR expressions
		}
		if result {
			return true, nil // Short-circuit on first true
		}
	}
	return false, nil // No expression was true
}

// WithAliases implements the Expression interface
func (e *OrExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same structure
	result := &OrExpression{
		Expressions: make([]storage.Expression, len(e.Expressions)),
		aliases:     aliases,
	}

	// Apply aliases to all child expressions
	for i, expr := range e.Expressions {
		if aliasable, ok := expr.(interface {
			WithAliases(map[string]string) storage.Expression
		}); ok {
			result.Expressions[i] = aliasable.WithAliases(aliases)
		} else {
			result.Expressions[i] = expr
		}
	}

	return result
}
