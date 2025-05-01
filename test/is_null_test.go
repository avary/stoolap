package test

import (
	"testing"

	"github.com/semihalev/stoolap/internal/storage"
	"github.com/semihalev/stoolap/internal/storage/expression"
)

func TestIsNullExpression(t *testing.T) {
	// Create a test schema
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER},
			{Name: "name", Type: storage.TEXT},
			{Name: "optional_value", Type: storage.INTEGER},
			{Name: "optional_text", Type: storage.TEXT},
		},
	}

	// Create test rows with NULL and non-NULL values
	row1 := []storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
		storage.NewNullValue(storage.INTEGER),
		storage.NewStringValue("text1"),
	}

	row2 := []storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
		storage.NewIntegerValue(42),
		storage.NewNullValue(storage.TEXT),
	}

	// Test IS NULL direct expression on row1
	t.Run("IS NULL on optional_value row1", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_value")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row1)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row1.optional_value IS NULL to be true, got false")
		}
	})

	// Test IS NOT NULL direct expression on row1
	t.Run("IS NOT NULL on optional_value row1", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_value")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNotNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row1)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row1.optional_value IS NOT NULL to be false, got true")
		}
	})

	// Test IS NULL direct expression on row2
	t.Run("IS NULL on optional_value row2", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_value")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row2.optional_value IS NULL to be false, got true")
		}
	})

	// Test IS NOT NULL direct expression on row2
	t.Run("IS NOT NULL on optional_value row2", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_value")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNotNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row2.optional_value IS NOT NULL to be true, got false")
		}
	})

	// Test IS NULL on optional_text with row2
	t.Run("IS NULL on optional_text row2", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_text")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row2.optional_text IS NULL to be true, got false")
		}
	})

	// Test IS NOT NULL on optional_text with row2
	t.Run("IS NOT NULL on optional_text row2", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_text")
		schemaAwareExpr := expression.NewSchemaAwareExpression(isNotNullExpr, schema)

		result, err := schemaAwareExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row2.optional_text IS NOT NULL to be false, got true")
		}
	})
}
