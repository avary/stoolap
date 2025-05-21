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
)

// parseCreateViewStatement parses a CREATE VIEW statement
func (p *Parser) parseCreateViewStatement() *CreateViewStatement {
	// Create a new CREATE VIEW statement with the CREATE token
	stmt := &CreateViewStatement{
		Token: p.curToken,
	}

	// Check for optional IF NOT EXISTS
	if p.peekTokenIsKeyword("IF") {
		p.nextToken() // Consume IF

		if !p.expectKeyword("NOT") {
			return nil
		}

		if !p.expectKeyword("EXISTS") {
			return nil
		}

		stmt.IfNotExists = true
	}

	// Parse view name
	if !p.expectPeek(TokenIdentifier) {
		p.addError(fmt.Sprintf("expected identifier as view name, got %s at %s", p.peekToken.Literal, p.peekToken.Position))
		return nil
	}

	// Create view name identifier
	stmt.ViewName = &Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}

	// Expect AS keyword
	if !p.expectKeyword("AS") {
		return nil
	}

	// Expect SELECT keyword
	if !p.expectKeyword("SELECT") {
		return nil
	}

	// Parse the SELECT statement
	selectStmt := p.parseSelectStatement()
	if selectStmt == nil {
		return nil
	}

	stmt.Query = selectStmt

	return stmt
}

// parseDropViewStatement parses a DROP VIEW statement
func (p *Parser) parseDropViewStatement() *DropViewStatement {
	// Create a new DROP VIEW statement with the DROP token
	stmt := &DropViewStatement{
		Token: p.curToken,
	}

	// Check for optional IF EXISTS
	if p.peekTokenIsKeyword("IF") {
		p.nextToken() // Consume IF

		if !p.expectKeyword("EXISTS") {
			return nil
		}

		stmt.IfExists = true
	}

	// Parse view name
	if !p.expectPeek(TokenIdentifier) {
		p.addError(fmt.Sprintf("expected identifier as view name, got %s at %s", p.peekToken.Literal, p.peekToken.Position))
		return nil
	}

	// Create view name identifier
	stmt.ViewName = &Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}

	return stmt
}
