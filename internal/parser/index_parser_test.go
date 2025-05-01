package parser

import (
	"testing"
)

func TestIndexParser(t *testing.T) {
	tests := []struct {
		input      string
		isUnique   bool
		isColumnar bool
	}{
		{"CREATE INDEX idx_users ON users (name)", false, false},
		{"CREATE UNIQUE INDEX idx_users ON users (name)", true, false},
		{"CREATE INDEX IF NOT EXISTS idx_users ON users (name)", false, false},
		{"CREATE UNIQUE INDEX IF NOT EXISTS idx_users ON users (name)", true, false},

		{"CREATE COLUMNAR INDEX ON users (name)", false, true},
		{"CREATE UNIQUE COLUMNAR INDEX ON users (name)", true, true},
		{"CREATE COLUMNAR INDEX IF NOT EXISTS ON users (name)", false, true},
		{"CREATE UNIQUE COLUMNAR INDEX IF NOT EXISTS ON users (name)", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := NewLexer(tt.input)
			p := NewParser(l)

			program := p.ParseProgram()
			if len(program.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(program.Statements))
			}

			stmt := program.Statements[0]

			if len(p.Errors()) > 0 {
				t.Errorf("parser has %d errors", len(p.Errors()))
				for _, err := range p.Errors() {
					t.Errorf("parser error: %s", err)
				}
				return
			}

			if tt.isColumnar {
				createStmt, ok := stmt.(*CreateColumnarIndexStatement)
				if !ok {
					t.Errorf("Expected CreateColumnarIndexStatement, got %T for %s", stmt, tt.input)
					return
				}

				if createStmt.IsUnique != tt.isUnique {
					t.Errorf("Expected IsUnique=%v, got %v for input: %s",
						tt.isUnique, createStmt.IsUnique, tt.input)
				}
			} else {
				createStmt, ok := stmt.(*CreateIndexStatement)
				if !ok {
					t.Errorf("Expected CreateIndexStatement, got %T for %s", stmt, tt.input)
					return
				}

				if createStmt.IsUnique != tt.isUnique {
					t.Errorf("Expected IsUnique=%v, got %v for input: %s",
						tt.isUnique, createStmt.IsUnique, tt.input)
				}
			}
		})
	}
}

func TestParseCreateIndex(t *testing.T) {
	// Test simple CREATE INDEX
	normal := "CREATE INDEX idx_test ON test (col)"
	l1 := NewLexer(normal)
	p1 := NewParser(l1)
	prog1 := p1.ParseProgram()

	if len(p1.Errors()) > 0 {
		t.Errorf("Errors parsing normal index: %v", p1.Errors())
	} else if len(prog1.Statements) == 0 {
		t.Errorf("No statements parsed for normal index")
	} else {
		stmt, ok := prog1.Statements[0].(*CreateIndexStatement)
		if !ok {
			t.Errorf("Expected CreateIndexStatement, got %T", prog1.Statements[0])
		} else if stmt.IsUnique {
			t.Errorf("Normal index incorrectly marked as unique")
		}
	}

	// Test CREATE UNIQUE INDEX
	unique := "CREATE UNIQUE INDEX idx_unique ON test (col)"
	l2 := NewLexer(unique)
	p2 := NewParser(l2)
	prog2 := p2.ParseProgram()

	if len(p2.Errors()) > 0 {
		t.Errorf("Errors parsing unique index: %v", p2.Errors())
	} else if len(prog2.Statements) == 0 {
		t.Errorf("No statements parsed for unique index")
	} else {
		stmt, ok := prog2.Statements[0].(*CreateIndexStatement)
		if !ok {
			t.Errorf("Expected CreateIndexStatement, got %T", prog2.Statements[0])
		} else if !stmt.IsUnique {
			t.Errorf("Unique index not marked as unique")
		}
	}

	// Test CREATE COLUMNAR INDEX
	columnar := "CREATE COLUMNAR INDEX ON test (col)"
	l3 := NewLexer(columnar)
	p3 := NewParser(l3)
	prog3 := p3.ParseProgram()

	if len(p3.Errors()) > 0 {
		t.Errorf("Errors parsing columnar index: %v", p3.Errors())
	} else if len(prog3.Statements) == 0 {
		t.Errorf("No statements parsed for columnar index")
	} else {
		stmt, ok := prog3.Statements[0].(*CreateColumnarIndexStatement)
		if !ok {
			t.Errorf("Expected CreateColumnarIndexStatement, got %T", prog3.Statements[0])
		} else if stmt.IsUnique {
			t.Errorf("Normal columnar index incorrectly marked as unique")
		}
	}

	// Test CREATE UNIQUE COLUMNAR INDEX
	uniqueColumnar := "CREATE UNIQUE COLUMNAR INDEX ON test (col)"
	l4 := NewLexer(uniqueColumnar)
	p4 := NewParser(l4)
	prog4 := p4.ParseProgram()

	if len(p4.Errors()) > 0 {
		t.Errorf("Errors parsing unique columnar index: %v", p4.Errors())
	} else if len(prog4.Statements) == 0 {
		t.Errorf("No statements parsed for unique columnar index")
	} else {
		stmt, ok := prog4.Statements[0].(*CreateColumnarIndexStatement)
		if !ok {
			t.Errorf("Expected CreateColumnarIndexStatement, got %T", prog4.Statements[0])
		} else if !stmt.IsUnique {
			t.Errorf("Unique columnar index not marked as unique")
		}
	}
}
