/* Copyright 2025 Stoolap Contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */

package test

import (
	"testing"

	"github.com/stoolap/stoolap/internal/parser"

	// Import required for function registration
	_ "github.com/stoolap/stoolap/internal/functions/aggregate"
	_ "github.com/stoolap/stoolap/internal/functions/scalar"
)

func TestParserDebug(t *testing.T) {
	// Test basic parsing
	query := "INSERT INTO departments (id, name) VALUES (1, 'Engineering')"

	lexer := parser.NewLexer(query)

	// Print all tokens
	t.Log("Tokens for query:", query)
	token := lexer.NextToken()
	for token.Type != parser.TokenEOF {
		t.Logf("Token: Type=%v, Literal=%s", token.Type, token.Literal)
		token = lexer.NextToken()
	}
}

func TestSimpleFunctionParsing(t *testing.T) {
	query := "SELECT SUM(amount) FROM sales"

	// Try with standard parser first
	t.Log("Standard Parser:")
	lexer := parser.NewLexer(query)
	p := parser.NewParser(lexer)

	// Parse the program
	_ = p.ParseProgram()

	if len(p.Errors()) > 0 {
		t.Logf("Standard Parser errors: %v", p.Errors())
	} else {
		t.Logf("Standard Parser parsed the query successfully")
	}

	// Now try with the function registry parser
	t.Log("\nFunction Registry Parser:")
	fp := parser.NewParser(parser.NewLexer(query))
	_ = fp.ParseProgram()

	if len(fp.Errors()) > 0 {
		t.Logf("Function Parser errors: %v", fp.Errors())
	} else {
		t.Logf("Function Parser parsed the query successfully")
	}
}

func TestFunctionParserComparison(t *testing.T) {
	query := "SELECT category, SUM(amount) AS total_sales FROM sales GROUP BY category ORDER BY total_sales DESC"

	// Try with standard parser first
	t.Log("Standard Parser:")
	lexer := parser.NewLexer(query)
	p := parser.NewParser(lexer)

	// Parse the program
	program := p.ParseProgram()

	if len(p.Errors()) > 0 {
		t.Logf("Standard Parser errors: %v", p.Errors())
	} else {
		t.Logf("Standard Parser parsed the query successfully")

		// Examine the AST
		if len(program.Statements) > 0 {
			selectStmt, ok := program.Statements[0].(*parser.SelectStatement)
			if ok {
				for i, col := range selectStmt.Columns {
					t.Logf("Column %d: %T %v", i, col, col)
				}

				for i, grp := range selectStmt.GroupBy {
					t.Logf("GROUP BY %d: %T %v", i, grp, grp)
				}

				for i, ord := range selectStmt.OrderBy {
					t.Logf("ORDER BY %d: %T %v", i, ord.Expression, ord.Expression)
				}
			}
		}
	}

	// Now try with the function registry parser
	t.Log("\nFunction Registry Parser:")
	fp := parser.NewParser(parser.NewLexer(query))
	fprogram := fp.ParseProgram()

	if len(fp.Errors()) > 0 {
		t.Logf("Function Parser errors: %v", fp.Errors())
	} else {
		t.Logf("Function Parser parsed the query successfully")

		// Examine the AST
		if len(fprogram.Statements) > 0 {
			selectStmt, ok := fprogram.Statements[0].(*parser.SelectStatement)
			if ok {
				for i, col := range selectStmt.Columns {
					t.Logf("Column %d: %T %v", i, col, col)
				}

				for i, grp := range selectStmt.GroupBy {
					t.Logf("GROUP BY %d: %T %v", i, grp, grp)
				}

				for i, ord := range selectStmt.OrderBy {
					t.Logf("ORDER BY %d: %T %v", i, ord.Expression, ord.Expression)
				}
			}
		}
	}
}

func TestAggregationParser(t *testing.T) {
	// Test basic query
	t.Run("Simple query", func(t *testing.T) {
		query := "SELECT id FROM sales LIMIT 1"

		lexer := parser.NewLexer(query)

		// Print all tokens
		t.Log("Tokens for: " + query)
		for {
			token := lexer.NextToken()
			t.Logf("Token: %+v", token)
			if token.Type == parser.TokenEOF {
				break
			}
		}
	})

	// Test COUNT query
	t.Run("COUNT query", func(t *testing.T) {
		query := "SELECT COUNT ( * ) FROM sales"

		// Use the parser with function registry instead of the basic parser
		p := parser.NewParser(parser.NewLexer(query))

		// Parse the program instead of individual statement
		t.Log("Parsing query:", query)
		countProgram := p.ParseProgram()
		if len(p.Errors()) > 0 {
			t.Fatalf("Parse errors: %v", p.Errors())
		}

		// Check that we have statements
		if len(countProgram.Statements) == 0 {
			t.Fatalf("No statements in program")
		}

		// Check the statement type
		selectStmt, ok := countProgram.Statements[0].(*parser.SelectStatement)
		if !ok {
			t.Fatalf("Statement is not a SELECT statement")
		}

		// Check the table expression
		if selectStmt.TableExpr == nil {
			t.Fatalf("No table expression in SELECT statement")
		}

		tableSource, ok := selectStmt.TableExpr.(*parser.SimpleTableSource)
		if !ok {
			t.Fatalf("Table expression is not a SimpleTableSource")
		}

		t.Logf("Table name: %s", tableSource.Name.Value)
	})

	// Test GROUP BY query
	t.Run("GROUP BY query", func(t *testing.T) {
		query := "SELECT category, COUNT ( * ) AS count FROM sales GROUP BY category"

		// Print all tokens
		lexer := parser.NewLexer(query)
		t.Log("Tokens for: " + query)
		for {
			token := lexer.NextToken()
			t.Logf("Token: %+v", token)
			if token.Type == parser.TokenEOF {
				break
			}
		}

		// Try parsing with functions
		p := parser.NewParser(parser.NewLexer(query))
		groupProgram := p.ParseProgram()
		if len(p.Errors()) > 0 {
			t.Logf("Parser errors: %v", p.Errors())
		} else {
			t.Logf("Parser successfully parsed the GROUP BY query")
			t.Logf("Statement count: %d", len(groupProgram.Statements))
		}
	})

	// Test our failing query
	t.Run("ORDER BY with alias query", func(t *testing.T) {
		query := "SELECT category, SUM(amount) AS total_sales FROM sales GROUP BY category ORDER BY total_sales DESC"

		// Try parsing the query with functions
		p := parser.NewParser(parser.NewLexer(query))

		// Parse the program
		aliasProgram := p.ParseProgram()

		if len(p.Errors()) > 0 {
			t.Logf("Parser errors: %v", p.Errors())
		} else {
			t.Logf("Parser parsed the query successfully")

			// Check that we have statements
			if len(aliasProgram.Statements) == 0 {
				t.Fatalf("No statements in program")
			}

			// Check the statement type
			selectStmt, ok := aliasProgram.Statements[0].(*parser.SelectStatement)
			if !ok {
				t.Fatalf("Statement is not a SELECT statement")
			}

			// Print column details
			for i, col := range selectStmt.Columns {
				t.Logf("Column %d: %T %v", i, col, col)
			}

			// Print GROUP BY details
			for i, grp := range selectStmt.GroupBy {
				t.Logf("GROUP BY %d: %T %v", i, grp, grp)
			}

			// Print ORDER BY details
			for i, ord := range selectStmt.OrderBy {
				t.Logf("ORDER BY %d: %T %v", i, ord.Expression, ord.Expression)
			}
		}

		// Now print the tokens for debugging
		tokenLexer := parser.NewLexer(query)
		t.Log("Tokens for: " + query)
		for {
			token := tokenLexer.NextToken()
			t.Logf("Token: %+v", token)
			if token.Type == parser.TokenEOF {
				break
			}
		}
	})
}
