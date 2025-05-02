package test

import (
	"context"
	"testing"

	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser"
	sql "github.com/stoolap/stoolap/internal/sql/executor"
)

func TestCastEvaluator(t *testing.T) {
	// Create an evaluator
	evaluator := sql.NewEvaluator(context.Background(), registry.GetGlobal())

	// Test cases
	tests := []struct {
		name     string
		expr     parser.Expression
		expected interface{}
	}{
		{
			name: "Cast string to int",
			expr: &parser.CastExpression{
				Expr:     &parser.StringLiteral{Value: "123"},
				TypeName: "INTEGER",
			},
			expected: int64(123),
		},
		{
			name: "Cast string to float",
			expr: &parser.CastExpression{
				Expr:     &parser.StringLiteral{Value: "3.14"},
				TypeName: "FLOAT",
			},
			expected: float64(3.14),
		},
		{
			name: "Cast int to string",
			expr: &parser.CastExpression{
				Expr:     &parser.IntegerLiteral{Value: 42},
				TypeName: "TEXT",
			},
			expected: "42",
		},
		{
			name: "Cast string to boolean",
			expr: &parser.CastExpression{
				Expr:     &parser.StringLiteral{Value: "true"},
				TypeName: "BOOLEAN",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Evaluate the expression
			result, err := evaluator.Evaluate(tt.expr)
			if err != nil {
				t.Fatalf("Error evaluating expression: %v", err)
			}

			t.Logf("Result: %v (type: %T)", result.AsInterface(), result.AsInterface())

			// Check the result type and value
			switch expected := tt.expected.(type) {
			case int64:
				if val, ok := result.AsInterface().(int64); !ok {
					t.Errorf("Expected int64, got %T", result.AsInterface())
				} else if val != expected {
					t.Errorf("Expected %d, got %d", expected, val)
				}
			case float64:
				if val, ok := result.AsInterface().(float64); !ok {
					t.Errorf("Expected float64, got %T", result.AsInterface())
				} else if val != expected {
					t.Errorf("Expected %f, got %f", expected, val)
				}
			case string:
				if val, ok := result.AsInterface().(string); !ok {
					t.Errorf("Expected string, got %T", result.AsInterface())
				} else if val != expected {
					t.Errorf("Expected %s, got %s", expected, val)
				}
			case bool:
				if val, ok := result.AsInterface().(bool); !ok {
					t.Errorf("Expected bool, got %T", result.AsInterface())
				} else if val != expected {
					t.Errorf("Expected %v, got %v", expected, val)
				}
			}
		})
	}
}
