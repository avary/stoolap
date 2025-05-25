/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package parser

import (
	"fmt"
	"testing"
)

func TestParseCastExpression(t *testing.T) {
	// Test direct parsing of a full statement with CAST expressions
	tests := []struct {
		input    string
		exprType string
	}{
		{"SELECT CAST(123 AS INTEGER);", "INTEGER"},
		{"SELECT CAST('hello' AS TEXT);", "TEXT"},
		{"SELECT CAST(column_name AS FLOAT);", "FLOAT"},
		{"SELECT CAST(NULL AS BOOLEAN);", "BOOLEAN"},
		{"SELECT CAST(date_col AS DATE);", "DATE"},
		{"SELECT CAST(json_col AS JSON);", "JSON"},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i), func(t *testing.T) {
			l := NewLexer(tt.input)
			p := NewParser(l)
			program := p.ParseProgram()

			if len(p.Errors()) > 0 {
				t.Errorf("parser has %d errors for input %s", len(p.Errors()), tt.input)
				for _, err := range p.Errors() {
					t.Errorf("parser error: %s", err)
				}
				return
			}

			if len(program.Statements) != 1 {
				t.Fatalf("program does not have 1 statement. got=%d", len(program.Statements))
			}

			selectStmt, ok := program.Statements[0].(*SelectStatement)
			if !ok {
				t.Fatalf("program.Statements[0] is not SelectStatement. got=%T", program.Statements[0])
			}

			if len(selectStmt.Columns) != 1 {
				t.Fatalf("select statement doesn't have 1 column, got=%d", len(selectStmt.Columns))
			}

			castExpr, ok := selectStmt.Columns[0].(*CastExpression)
			if !ok {
				t.Fatalf("column is not CastExpression. got=%T", selectStmt.Columns[0])
			}

			if castExpr.TypeName != tt.exprType {
				t.Errorf("castExpr.TypeName not %s. got=%s", tt.exprType, castExpr.TypeName)
			}
		})
	}
}
