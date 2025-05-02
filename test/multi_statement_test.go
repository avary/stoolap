package test

import (
	"testing"

	"github.com/stoolap/stoolap/internal/parser"
)

func TestMultiStatementParsing(t *testing.T) {
	// Test a multi-statement query with transaction statements
	multiStmt := `
BEGIN TRANSACTION;
INSERT INTO users (id, name) VALUES (1, 'Alice');
COMMIT;
`

	p := parser.NewParser(parser.NewLexer(multiStmt))
	program := p.ParseProgram()

	// Check that we have multiple statements
	if len(program.Statements) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(program.Statements))
	}

	// Check the types of statements
	if len(program.Statements) >= 3 {
		_, ok1 := program.Statements[0].(*parser.BeginStatement)
		_, ok2 := program.Statements[1].(*parser.InsertStatement)
		_, ok3 := program.Statements[2].(*parser.CommitStatement)

		if !ok1 {
			t.Errorf("First statement is not a BEGIN statement, got %T", program.Statements[0])
		}

		if !ok2 {
			t.Errorf("Second statement is not an INSERT statement, got %T", program.Statements[1])
		}

		if !ok3 {
			t.Errorf("Third statement is not a COMMIT statement, got %T", program.Statements[2])
		}
	}

	// Check for any parse errors
	if len(p.Errors()) > 0 {
		t.Logf("Parse errors: %v", p.Errors())
	}
}
