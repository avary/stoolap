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

package parser

import (
	"fmt"
	"testing"
)

// TestParserErrors tests that parser errors are reported correctly
func TestParserErrors(t *testing.T) {
	input := "SELET * FROM users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	parser.ParseProgram()
	errors := parser.Errors()

	if len(errors) == 0 {
		t.Errorf("parser didn't report any errors for invalid input")
	}
}

// TestExpressionParsing tests parsing of expressions through full SQL statements
func TestExpressionParsing(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"SELECT 1 + 2 + 3 FROM dual"},
		{"SELECT id + 5 FROM users"},
		{"SELECT -15 FROM users"},
		{"SELECT 'hello' FROM users"},
		{"SELECT id FROM users WHERE id > 5"},
		// Skipping multiplication test for now: {"SELECT 5 * 10 FROM users"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)

			program := parser.ParseProgram()
			checkParserErrors(t, parser)

			// If we didn't get any errors, that's good enough for this test
			if len(program.Statements) != 1 {
				t.Fatalf("program has wrong number of statements. got=%d", len(program.Statements))
			}

			// Verify we got a select statement
			_, ok := program.Statements[0].(*SelectStatement)
			if !ok {
				t.Fatalf("program.Statements[0] is not SelectStatement. got=%T", program.Statements[0])
			}
		})
	}
}

// TestJoinParsing tests parsing of JOIN expressions
func TestJoinParsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		joinType string
	}{
		{
			"Inner Join",
			"SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id",
			"INNER",
		},
		{
			"Left Join",
			"SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id",
			"LEFT",
		},
		{
			"Right Join",
			"SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id",
			"RIGHT",
		},
		{
			"Full Join",
			"SELECT users.name, orders.product FROM users FULL JOIN orders ON users.id = orders.user_id",
			"FULL",
		},
		{
			"Cross Join",
			"SELECT users.name, orders.product FROM users CROSS JOIN orders",
			"CROSS",
		},
		{
			"Simple Join",
			"SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id",
			"INNER", // Default join type is INNER
		},
		{
			"Multiple Joins",
			"SELECT u.name, o.product, a.street FROM users u JOIN orders o ON u.id = o.user_id JOIN addresses a ON u.id = a.user_id",
			"INNER", // We'll only check the first join type
		},
		{
			"Joins with Aliases",
			"SELECT u.name, o.product FROM users AS u JOIN orders AS o ON u.id = o.user_id",
			"INNER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)

			program := parser.ParseProgram()
			checkParserErrors(t, parser)

			// If we didn't get any errors, check that the join type matches expected
			if len(program.Statements) != 1 {
				t.Fatalf("program has wrong number of statements. got=%d", len(program.Statements))
			}

			// Verify we got a select statement
			stmt, ok := program.Statements[0].(*SelectStatement)
			if !ok {
				t.Fatalf("program.Statements[0] is not SelectStatement. got=%T", program.Statements[0])
			}

			// Check that the table expression is a JoinTableSource
			joinStmt, ok := stmt.TableExpr.(*JoinTableSource)
			if !ok {
				t.Fatalf("stmt.TableExpr is not JoinTableSource. got=%T", stmt.TableExpr)
			}

			// Check join type
			if joinStmt.JoinType != tt.joinType {
				t.Errorf("wrong join type. expected=%s, got=%s", tt.joinType, joinStmt.JoinType)
			}

			// Verify the structure is correctly formed
			if joinStmt.Left == nil || joinStmt.Right == nil {
				t.Errorf("join missing left or right tables. left=%v, right=%v", joinStmt.Left, joinStmt.Right)
			}

			// For non-CROSS joins, verify there's a condition
			if joinStmt.JoinType != "CROSS" && joinStmt.Condition == nil {
				t.Errorf("non-CROSS join missing condition")
			}
		})
	}
}

// TestParserSimpleStatements tests that simple SQL statements can be parsed
func TestParserSimpleStatements(t *testing.T) {
	tests := []struct {
		input    string
		stmtType string
	}{
		{"SELECT * FROM users", "SelectStatement"},
		{"CREATE TABLE users (id INTEGER, name TEXT)", "CreateTableStatement"},
		{"DROP TABLE users", "DropTableStatement"},
		{"INSERT INTO users VALUES (1, 'John')", "InsertStatement"},
		{"UPDATE users SET name = 'Jane' WHERE id = 1", "UpdateStatement"},
		{"DELETE FROM users WHERE id = 1", "DeleteStatement"},
		{"ALTER TABLE users ADD COLUMN email TEXT", "AlterTableStatement"},
	}

	for _, tt := range tests {
		t.Run(tt.stmtType, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)

			program := parser.ParseProgram()
			checkParserErrors(t, parser)

			if len(program.Statements) != 1 {
				t.Fatalf("program.Statements does not contain 1 statement. got=%d",
					len(program.Statements))
			}

			typeName := getStatementTypeName(program.Statements[0])
			if typeName != tt.stmtType {
				t.Fatalf("statement type is not %s. got=%s", tt.stmtType, typeName)
			}
		})
	}
}

// TestSelectStatementParsing tests parsing of SELECT statements
func TestSelectStatementParsing(t *testing.T) {
	tests := []struct {
		input    string
		numCols  int
		firstCol string
		hasFrom  bool
	}{
		{
			"SELECT id, name FROM users",
			2,
			"id",
			true,
		},
		{
			"SELECT * FROM users",
			1,
			"*",
			true,
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		parser := NewParser(lexer)

		program := parser.ParseProgram()
		checkParserErrors(t, parser)

		if len(program.Statements) != 1 {
			t.Fatalf("program.Statements does not contain 1 statement. got=%d",
				len(program.Statements))
		}

		stmt, ok := program.Statements[0].(*SelectStatement)
		if !ok {
			t.Fatalf("program.Statements[0] is not *SelectStatement. got=%T",
				program.Statements[0])
		}

		if len(stmt.Columns) != tt.numCols {
			t.Fatalf("stmt.Columns does not contain %d columns. got=%d",
				tt.numCols, len(stmt.Columns))
		}

		firstCol := stmt.Columns[0].String()
		if firstCol != tt.firstCol {
			t.Fatalf("first column is not %q. got=%q",
				tt.firstCol, firstCol)
		}

		if tt.hasFrom && stmt.TableExpr == nil {
			t.Fatalf("expected FROM clause, but got nil TableExpr")
		}
	}
}

// TestCreateTableStatementParsing tests parsing of CREATE TABLE statements
func TestCreateTableStatementParsing(t *testing.T) {
	input := "CREATE TABLE users (id INTEGER, name TEXT)"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	program := parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d",
			len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*CreateTableStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not *CreateTableStatement. got=%T",
			program.Statements[0])
	}

	if stmt.TableName.Value != "users" {
		t.Fatalf("stmt.TableName.Value not 'users'. got=%q", stmt.TableName.Value)
	}

	if len(stmt.Columns) != 2 {
		t.Fatalf("stmt.Columns does not contain 2 columns. got=%d",
			len(stmt.Columns))
	}

	tests := []struct {
		expectedName string
		expectedType string
	}{
		{"id", "INTEGER"},
		{"name", "TEXT"},
	}

	for i, tt := range tests {
		column := stmt.Columns[i]
		if column.Name.Value != tt.expectedName {
			t.Errorf("column[%d].Name.Value not %q. got=%q",
				i, tt.expectedName, column.Name.Value)
		}

		if column.Type != tt.expectedType {
			t.Errorf("column[%d].Type not %q. got=%q",
				i, tt.expectedType, column.Type)
		}
	}
}

// TestDropTableStatementParsing tests parsing of DROP TABLE statements
func TestDropTableStatementParsing(t *testing.T) {
	input := "DROP TABLE users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	program := parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d",
			len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*DropTableStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not *DropTableStatement. got=%T",
			program.Statements[0])
	}

	if stmt.TableName.Value != "users" {
		t.Fatalf("stmt.TableName.Value not 'users'. got=%q", stmt.TableName.Value)
	}
}

// TestInsertStatementParsing tests parsing of INSERT statements
func TestInsertStatementParsing(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'John')"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	program := parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d",
			len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*InsertStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not *InsertStatement. got=%T",
			program.Statements[0])
	}

	if stmt.TableName.Value != "users" {
		t.Fatalf("stmt.TableName.Value not 'users'. got=%q", stmt.TableName.Value)
	}

	if len(stmt.Values) != 1 {
		t.Fatalf("stmt.Values does not contain 1 row. got=%d",
			len(stmt.Values))
	}

	row := stmt.Values[0]
	if len(row) != 2 {
		t.Fatalf("row does not contain 2 values. got=%d", len(row))
	}
}

// TestUpdateStatementParsing tests parsing of UPDATE statements
func TestUpdateStatementParsing(t *testing.T) {
	input := "UPDATE users SET name = 'Jane Doe', age = 32 WHERE id = 1"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	program := parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*UpdateStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not UpdateStatement. got=%T", program.Statements[0])
	}

	if stmt.TableName.Value != "users" {
		t.Errorf("stmt.TableName.Value not 'users'. got=%q", stmt.TableName.Value)
	}

	if len(stmt.Updates) != 2 {
		t.Errorf("stmt.Updates length not 2. got=%d", len(stmt.Updates))
	}

	// Check name update
	if nameExpr, ok := stmt.Updates["name"]; ok {
		stringLit, ok := nameExpr.(*StringLiteral)
		if !ok {
			t.Errorf("name value is not StringLiteral. got=%T", nameExpr)
		} else if stringLit.Value != "Jane Doe" {
			t.Errorf("name value not 'Jane Doe'. got=%q", stringLit.Value)
		}
	} else {
		t.Errorf("name not found in updates")
	}

	// Check age update
	if ageExpr, ok := stmt.Updates["age"]; ok {
		intLit, ok := ageExpr.(*IntegerLiteral)
		if !ok {
			t.Errorf("age value is not IntegerLiteral. got=%T", ageExpr)
		} else if intLit.Value != 32 {
			t.Errorf("age value not 32. got=%d", intLit.Value)
		}
	} else {
		t.Errorf("age not found in updates")
	}

	// Check WHERE clause
	if stmt.Where == nil {
		t.Errorf("WHERE clause is nil")
	} else {
		infix, ok := stmt.Where.(*InfixExpression)
		if !ok {
			t.Errorf("WHERE is not InfixExpression. got=%T", stmt.Where)
		} else {
			if infix.Operator != "=" {
				t.Errorf("WHERE operator not '='. got=%q", infix.Operator)
			}

			left, ok := infix.Left.(*Identifier)
			if !ok {
				t.Errorf("WHERE left not Identifier. got=%T", infix.Left)
			} else if left.Value != "id" {
				t.Errorf("WHERE left value not 'id'. got=%q", left.Value)
			}

			right, ok := infix.Right.(*IntegerLiteral)
			if !ok {
				t.Errorf("WHERE right not IntegerLiteral. got=%T", infix.Right)
			} else if right.Value != 1 {
				t.Errorf("WHERE right value not 1. got=%d", right.Value)
			}
		}
	}

	// Test with complex expression
	input = "UPDATE employees SET salary = salary * 1.1 WHERE is_active = TRUE"
	lexer = NewLexer(input)
	parser = NewParser(lexer)

	program = parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	stmt, ok = program.Statements[0].(*UpdateStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not UpdateStatement. got=%T", program.Statements[0])
	}

	// Check complex expression (salary * 1.1)
	if salaryExpr, ok := stmt.Updates["salary"]; ok {
		infix, ok := salaryExpr.(*InfixExpression)
		if !ok {
			t.Errorf("salary value is not InfixExpression. got=%T", salaryExpr)
		} else {
			if infix.Operator != "*" {
				t.Errorf("Expression operator not '*'. got=%q", infix.Operator)
			}

			left, ok := infix.Left.(*Identifier)
			if !ok {
				t.Errorf("Expression left not Identifier. got=%T", infix.Left)
			} else if left.Value != "salary" {
				t.Errorf("Expression left value not 'salary'. got=%q", left.Value)
			}

			right, ok := infix.Right.(*FloatLiteral)
			if !ok {
				t.Errorf("Expression right not FloatLiteral. got=%T", infix.Right)
			} else if right.Value != 1.1 {
				t.Errorf("Expression right value not 1.1. got=%f", right.Value)
			}
		}
	} else {
		t.Errorf("salary not found in updates")
	}
}

// TestDeleteStatementParsing tests parsing of DELETE statements
func TestDeleteStatementParsing(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"

	lexer := NewLexer(input)
	parser := NewParser(lexer)

	program := parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*DeleteStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not DeleteStatement. got=%T", program.Statements[0])
	}

	if stmt.TableName.Value != "users" {
		t.Errorf("stmt.TableName.Value not 'users'. got=%q", stmt.TableName.Value)
	}

	// Check WHERE clause
	if stmt.Where == nil {
		t.Errorf("WHERE clause is nil")
	} else {
		infix, ok := stmt.Where.(*InfixExpression)
		if !ok {
			t.Errorf("WHERE is not InfixExpression. got=%T", stmt.Where)
		} else {
			if infix.Operator != "=" {
				t.Errorf("WHERE operator not '='. got=%q", infix.Operator)
			}

			left, ok := infix.Left.(*Identifier)
			if !ok {
				t.Errorf("WHERE left not Identifier. got=%T", infix.Left)
			} else if left.Value != "id" {
				t.Errorf("WHERE left value not 'id'. got=%q", left.Value)
			}

			right, ok := infix.Right.(*IntegerLiteral)
			if !ok {
				t.Errorf("WHERE right not IntegerLiteral. got=%T", infix.Right)
			} else if right.Value != 1 {
				t.Errorf("WHERE right value not 1. got=%d", right.Value)
			}
		}
	}

	// Test with complex WHERE condition
	input = "DELETE FROM users WHERE age > 30 AND is_active = FALSE"
	lexer = NewLexer(input)
	parser = NewParser(lexer)

	program = parser.ParseProgram()
	checkParserErrors(t, parser)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	stmt, ok = program.Statements[0].(*DeleteStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not DeleteStatement. got=%T", program.Statements[0])
	}

	// Check complex WHERE clause (age > 30 AND is_active = FALSE)
	if stmt.Where == nil {
		t.Errorf("WHERE clause is nil")
	} else {
		infix, ok := stmt.Where.(*InfixExpression)
		if !ok {
			t.Errorf("WHERE is not InfixExpression. got=%T", stmt.Where)
		} else {
			if infix.Operator != "AND" {
				t.Errorf("Outer WHERE operator not 'AND'. got=%q", infix.Operator)
			}

			// Check left side (age > 30)
			leftInfix, ok := infix.Left.(*InfixExpression)
			if !ok {
				t.Errorf("WHERE left not InfixExpression. got=%T", infix.Left)
			} else {
				if leftInfix.Operator != ">" {
					t.Errorf("Left WHERE operator not '>'. got=%q", leftInfix.Operator)
				}

				leftIdent, ok := leftInfix.Left.(*Identifier)
				if !ok {
					t.Errorf("Left WHERE left not Identifier. got=%T", leftInfix.Left)
				} else if leftIdent.Value != "age" {
					t.Errorf("Left WHERE left value not 'age'. got=%q", leftIdent.Value)
				}

				leftValue, ok := leftInfix.Right.(*IntegerLiteral)
				if !ok {
					t.Errorf("Left WHERE right not IntegerLiteral. got=%T", leftInfix.Right)
				} else if leftValue.Value != 30 {
					t.Errorf("Left WHERE right value not 30. got=%d", leftValue.Value)
				}
			}

			// Check right side (is_active = FALSE)
			rightInfix, ok := infix.Right.(*InfixExpression)
			if !ok {
				t.Errorf("WHERE right not InfixExpression. got=%T", infix.Right)
			} else {
				if rightInfix.Operator != "=" {
					t.Errorf("Right WHERE operator not '='. got=%q", rightInfix.Operator)
				}

				rightIdent, ok := rightInfix.Left.(*Identifier)
				if !ok {
					t.Errorf("Right WHERE left not Identifier. got=%T", rightInfix.Left)
				} else if rightIdent.Value != "is_active" {
					t.Errorf("Right WHERE left value not 'is_active'. got=%q", rightIdent.Value)
				}

				rightValue, ok := rightInfix.Right.(*BooleanLiteral)
				if !ok {
					t.Errorf("Right WHERE right not BooleanLiteral. got=%T", rightInfix.Right)
				} else if rightValue.Value != false {
					t.Errorf("Right WHERE right value not FALSE. got=%t", rightValue.Value)
				}
			}
		}
	}
}

// TestAlterTableStatementParsing tests parsing of ALTER TABLE statements
func TestAlterTableStatementParsing(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		operation    AlterTableOperation
		tableName    string
		columnName   string
		newColName   string
		newTableName string
		columnType   string
	}{
		{
			name:       "Add Column",
			input:      "ALTER TABLE users ADD COLUMN email TEXT",
			operation:  AddColumn,
			tableName:  "users",
			columnName: "email",
			columnType: "TEXT",
		},
		{
			name:       "Add Column Without COLUMN Keyword",
			input:      "ALTER TABLE users ADD email TEXT",
			operation:  AddColumn,
			tableName:  "users",
			columnName: "email",
			columnType: "TEXT",
		},
		{
			name:       "Drop Column",
			input:      "ALTER TABLE users DROP COLUMN email",
			operation:  DropColumn,
			tableName:  "users",
			columnName: "email",
		},
		{
			name:       "Drop Column Without COLUMN Keyword",
			input:      "ALTER TABLE users DROP email",
			operation:  DropColumn,
			tableName:  "users",
			columnName: "email",
		},
		{
			name:       "Rename Column",
			input:      "ALTER TABLE users RENAME COLUMN email TO contact_email",
			operation:  RenameColumn,
			tableName:  "users",
			columnName: "email",
			newColName: "contact_email",
		},
		{
			name:         "Rename Table",
			input:        "ALTER TABLE users RENAME TO customers",
			operation:    RenameTable,
			tableName:    "users",
			newTableName: "customers",
		},
		{
			name:       "Modify Column",
			input:      "ALTER TABLE users MODIFY COLUMN email VARCHAR",
			operation:  ModifyColumn,
			tableName:  "users",
			columnName: "email",
			columnType: "VARCHAR",
		},
		{
			name:       "Modify Column Without COLUMN Keyword",
			input:      "ALTER TABLE users MODIFY email VARCHAR",
			operation:  ModifyColumn,
			tableName:  "users",
			columnName: "email",
			columnType: "VARCHAR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)

			program := parser.ParseProgram()
			checkParserErrors(t, parser)

			if len(program.Statements) != 1 {
				t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
			}

			stmt, ok := program.Statements[0].(*AlterTableStatement)
			if !ok {
				t.Fatalf("program.Statements[0] is not AlterTableStatement. got=%T", program.Statements[0])
			}

			if stmt.TableName.Value != tt.tableName {
				t.Errorf("stmt.TableName.Value not '%s'. got=%q", tt.tableName, stmt.TableName.Value)
			}

			if stmt.Operation != tt.operation {
				t.Errorf("stmt.Operation not %v. got=%v", tt.operation, stmt.Operation)
			}

			switch tt.operation {
			case AddColumn, ModifyColumn:
				if stmt.ColumnDef == nil {
					t.Fatalf("stmt.ColumnDef is nil")
				}

				if stmt.ColumnDef.Name.Value != tt.columnName {
					t.Errorf("stmt.ColumnDef.Name.Value not '%s'. got=%q", tt.columnName, stmt.ColumnDef.Name.Value)
				}

				if stmt.ColumnDef.Type != tt.columnType {
					t.Errorf("stmt.ColumnDef.Type not '%s'. got=%q", tt.columnType, stmt.ColumnDef.Type)
				}

			case DropColumn:
				if stmt.ColumnName == nil {
					t.Fatalf("stmt.ColumnName is nil")
				}

				if stmt.ColumnName.Value != tt.columnName {
					t.Errorf("stmt.ColumnName.Value not '%s'. got=%q", tt.columnName, stmt.ColumnName.Value)
				}

			case RenameColumn:
				if stmt.ColumnName == nil {
					t.Fatalf("stmt.ColumnName is nil")
				}

				if stmt.ColumnName.Value != tt.columnName {
					t.Errorf("stmt.ColumnName.Value not '%s'. got=%q", tt.columnName, stmt.ColumnName.Value)
				}

				if stmt.NewColumnName == nil {
					t.Fatalf("stmt.NewColumnName is nil")
				}

				if stmt.NewColumnName.Value != tt.newColName {
					t.Errorf("stmt.NewColumnName.Value not '%s'. got=%q", tt.newColName, stmt.NewColumnName.Value)
				}

			case RenameTable:
				if stmt.NewTableName == nil {
					t.Fatalf("stmt.NewTableName is nil")
				}

				if stmt.NewTableName.Value != tt.newTableName {
					t.Errorf("stmt.NewTableName.Value not '%s'. got=%q", tt.newTableName, stmt.NewTableName.Value)
				}
			}
		})
	}
}

// Helper functions

func checkParserErrors(t *testing.T, p *Parser) {
	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))
	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}
	t.FailNow()
}

// TestDropIndexStatementParsing tests parsing of DROP INDEX statements
func TestDropIndexStatementParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		ifExists  bool
		indexName string
		tableName string
	}{
		{
			name:      "Basic Drop Index",
			input:     "DROP INDEX idx_users_id ON users",
			ifExists:  false,
			indexName: "idx_users_id",
			tableName: "users",
		},
		{
			name:      "Drop Index If Exists",
			input:     "DROP INDEX IF EXISTS idx_users_id ON users",
			ifExists:  true,
			indexName: "idx_users_id",
			tableName: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			parser := NewParser(lexer)

			program := parser.ParseProgram()
			checkParserErrors(t, parser)

			if len(program.Statements) != 1 {
				t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
			}

			stmt, ok := program.Statements[0].(*DropIndexStatement)
			if !ok {
				t.Fatalf("program.Statements[0] is not DropIndexStatement. got=%T", program.Statements[0])
			}

			if stmt.IfExists != tt.ifExists {
				t.Errorf("stmt.IfExists not %v. got=%v", tt.ifExists, stmt.IfExists)
			}

			if stmt.IndexName.Value != tt.indexName {
				t.Errorf("stmt.IndexName.Value not '%s'. got=%q", tt.indexName, stmt.IndexName.Value)
			}

			if stmt.TableName.Value != tt.tableName {
				t.Errorf("stmt.TableName.Value not '%s'. got=%q", tt.tableName, stmt.TableName.Value)
			}
		})
	}
}

// Helper functions are now reduced to only those we need

func getStatementTypeName(stmt Statement) string {
	switch stmt.(type) {
	case *SelectStatement:
		return "SelectStatement"
	case *CreateTableStatement:
		return "CreateTableStatement"
	case *DropTableStatement:
		return "DropTableStatement"
	case *InsertStatement:
		return "InsertStatement"
	case *UpdateStatement:
		return "UpdateStatement"
	case *DeleteStatement:
		return "DeleteStatement"
	case *AlterTableStatement:
		return "AlterTableStatement"
	case *DropIndexStatement:
		return "DropIndexStatement"
	default:
		return fmt.Sprintf("UnknownStatement: %T", stmt)
	}
}
