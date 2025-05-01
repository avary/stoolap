package test

import (
	"context"
	"testing"

	"github.com/semihalev/stoolap/pkg"
)

// TestCastSpecialHelper creates a specialized test to diagnose the CAST issue
func TestCastSpecialHelper(t *testing.T) {
	// Create a test database
	ctx := context.Background()
	db, _ := pkg.Open("db://var/tmp/test_cast_special_helper.db")
	defer db.Close()

	// Get the SQL executor
	exec := db.Executor()

	// Create a test table with test data
	exec.Execute(ctx, nil, `
		CREATE TABLE cast_special_helper (
			id INTEGER PRIMARY KEY,
			text_val TEXT
		)
	`)

	exec.Execute(ctx, nil, `
		INSERT INTO cast_special_helper (id, text_val) VALUES
		(1, '123'),
		(2, '456')
	`)

	// First verify CAST works in SELECT
	selectResult, _ := exec.Execute(ctx, nil, `SELECT id, CAST(text_val AS INTEGER) FROM cast_special_helper`)
	t.Log("SELECT with CAST results:")
	for selectResult.Next() {
		var id int
		var castVal int
		selectResult.Scan(&id, &castVal)
		t.Logf("  Row %d: CAST(text_val AS INTEGER) = %d", id, castVal)
	}

	// Now try our where clause fix
	// This is what would typically fail
	whereResult, _ := exec.Execute(ctx, nil, `
		SELECT id FROM cast_special_helper 
		WHERE CAST(text_val AS INTEGER) > 200
	`)

	t.Log("WHERE with CAST results:")
	matchCount := 0
	for whereResult.Next() {
		var id int
		whereResult.Scan(&id)
		t.Logf("  Row %d matches WHERE CAST(text_val AS INTEGER) > 200", id)
		matchCount++
	}

	if matchCount == 0 {
		t.Errorf("Expected at least one match for WHERE clause with CAST")
	}

	// Debug test to see what values we actually get in queries
	t.Log("\nDiagnostic tests:")

	// Test 1: Get text_val directly
	for id := 1; id <= 2; id++ {
		idStr := ""
		if id == 1 {
			idStr = "1"
		} else {
			idStr = "2"
		}
		directResult, _ := exec.Execute(ctx, nil, "SELECT text_val FROM cast_special_helper WHERE id = "+idStr)
		if directResult.Next() {
			var textVal string
			directResult.Scan(&textVal)
			t.Logf("  Direct query for id=%d: text_val=%q", id, textVal)
		}
	}

	// Test 2: Use CAST in SELECT
	castSelectResult, _ := exec.Execute(ctx, nil, "SELECT id, text_val, CAST(text_val AS INTEGER) FROM cast_special_helper")
	t.Log("  CAST in SELECT results:")
	for castSelectResult.Next() {
		var id int
		var textVal string
		var castVal int
		castSelectResult.Scan(&id, &textVal, &castVal)
		t.Logf("    Row id=%d: text_val=%q, CAST result=%d", id, textVal, castVal)
	}

	// Recommendation for patching the system:
	t.Log("\nRecommended fix:")
	t.Log("1. In filtered_result.go when processing column values, store the original StringValue")
	t.Log("2. In evaluator.go when handling CastExpression, check if expr.Expr is an Identifier")
	t.Log("3. If it is, and the value is an empty string, try to get the original StringValue")
	t.Log("4. Use the original StringValue's AsString() method to get the actual value")
	t.Log("5. This preserves the correct string value for CAST operations in WHERE clauses")
}
