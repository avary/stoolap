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
	"strings"
)

// Transaction statement parsers

// parseBeginStatement parses a BEGIN TRANSACTION [ISOLATION LEVEL {level}] statement
func (p *Parser) parseBeginStatement() *BeginStatement {
	// Create a new BEGIN statement with the BEGIN token
	stmt := &BeginStatement{
		Token: p.curToken,
	}

	// Ensure peekToken is valid and not nil
	if p.peekToken.Type == TokenEOF {
		// Just return the basic BEGIN statement if we've reached EOF
		return stmt
	}

	// Check for optional TRANSACTION keyword
	if p.peekTokenIsKeyword("TRANSACTION") {
		p.nextToken() // Consume TRANSACTION
	}

	// Check for optional ISOLATION LEVEL clause
	if p.peekTokenIsKeyword("ISOLATION") {
		p.nextToken() // Consume ISOLATION

		// Check if the next token is LEVEL
		if !p.expectPeek(TokenKeyword) || !p.curTokenIsKeyword("LEVEL") {
			p.addError(fmt.Sprintf("expected LEVEL after ISOLATION, got %s at %s",
				p.curToken.Literal, p.curToken.Position))
			return nil
		}

		// Check for isolation level value
		if !p.expectPeek(TokenKeyword) {
			p.addError(fmt.Sprintf("expected isolation level, got %s at %s",
				p.peekToken.Literal, p.peekToken.Position))
			return nil
		}

		// Parse the isolation level
		isolationLevel := p.curToken.Literal

		// Validate the isolation level
		isValid := false

		// Check for single-word levels: SERIALIZABLE
		if isolationLevel == "SERIALIZABLE" {
			stmt.IsolationLevel = isolationLevel
			isValid = true
		} else if isolationLevel == "REPEATABLE" {
			// Check for REPEATABLE READ
			if p.peekTokenIsKeyword("READ") {
				p.nextToken() // Consume READ
				stmt.IsolationLevel = "REPEATABLE READ"
				isValid = true
			}
		} else if isolationLevel == "READ" {
			// Check for READ UNCOMMITTED or READ COMMITTED
			if p.peekTokenIsKeyword("UNCOMMITTED") {
				p.nextToken() // Consume UNCOMMITTED
				stmt.IsolationLevel = "READ UNCOMMITTED"
				isValid = true
			} else if p.peekTokenIsKeyword("COMMITTED") {
				p.nextToken() // Consume COMMITTED
				stmt.IsolationLevel = "READ COMMITTED"
				isValid = true
			}
		}

		if !isValid {
			p.addError(fmt.Sprintf("invalid isolation level: %s at %s",
				isolationLevel, p.curToken.Position))
			return nil
		}
	}

	return stmt
}

// parseCommitStatement parses a COMMIT statement
func (p *Parser) parseCommitStatement() *CommitStatement {
	// Create a new COMMIT statement with the COMMIT token
	stmt := &CommitStatement{
		Token: p.curToken,
	}

	// Ensure peekToken is valid and not nil
	if p.peekToken.Type == TokenEOF {
		// Just return the basic COMMIT statement if we've reached EOF
		return stmt
	}

	// Check for optional TRANSACTION keyword
	if p.peekTokenIsKeyword("TRANSACTION") {
		p.nextToken() // Consume TRANSACTION
	}

	return stmt
}

// parseRollbackStatement parses a ROLLBACK [TO SAVEPOINT savepoint_name] statement
func (p *Parser) parseRollbackStatement() *RollbackStatement {
	// Create a new ROLLBACK statement with the ROLLBACK token
	stmt := &RollbackStatement{
		Token: p.curToken,
	}

	// Ensure peekToken is valid and not nil
	if p.peekToken.Type == TokenEOF {
		// Just return the basic ROLLBACK statement if we've reached EOF
		return stmt
	}

	// Check for optional TRANSACTION keyword
	if p.peekTokenIsKeyword("TRANSACTION") {
		p.nextToken() // Consume TRANSACTION

		// Check for EOF after consuming TRANSACTION
		if p.peekToken.Type == TokenEOF {
			return stmt
		}
	}

	// Check for optional TO SAVEPOINT clause
	if p.peekTokenIsKeyword("TO") {
		p.nextToken() // Consume TO

		// Check for EOF after consuming TO
		if p.peekToken.Type == TokenEOF {
			return stmt
		}

		// Check for SAVEPOINT keyword (optional in some dialects)
		if p.peekTokenIsKeyword("SAVEPOINT") {
			p.nextToken() // Consume SAVEPOINT

			// Check for EOF after consuming SAVEPOINT
			if p.peekToken.Type == TokenEOF {
				return stmt
			}
		}

		// Parse savepoint name
		if !p.expectPeek(TokenIdentifier) {
			p.addError(fmt.Sprintf("expected identifier as savepoint name, got %s at %s",
				p.peekToken.Literal, p.peekToken.Position))
			return nil
		}

		// Create savepoint identifier
		stmt.SavepointName = &Identifier{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}
	}

	return stmt
}

// parseSavepointStatement parses a SAVEPOINT statement
func (p *Parser) parseSavepointStatement() *SavepointStatement {
	// Create a new SAVEPOINT statement with the SAVEPOINT token
	stmt := &SavepointStatement{
		Token: p.curToken,
	}

	// Ensure peekToken is valid
	if p.peekToken.Type == TokenEOF {
		p.addError(fmt.Sprintf("unexpected end of file after SAVEPOINT at %s", p.curToken.Position))
		return nil
	}

	// Parse savepoint name
	if !p.expectPeek(TokenIdentifier) {
		p.addError(fmt.Sprintf("expected identifier as savepoint name, got %s at %s",
			p.peekToken.Literal, p.peekToken.Position))
		return nil
	}

	// Create savepoint identifier
	stmt.SavepointName = &Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}

	return stmt
}

// parseSetStatement parses a SET statement for session variables
func (p *Parser) parseSetStatement() *SetStatement {
	// Create a new SET statement with the SET token
	stmt := &SetStatement{
		Token: p.curToken,
	}

	// Move past the SET token
	p.nextToken()

	// Parse the variable name
	if !p.curTokenIs(TokenIdentifier) {
		p.addError(fmt.Sprintf("expected identifier for variable name at %s", p.curToken.Position))
		return nil
	}

	stmt.Name = &Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}

	// Move past the variable name
	p.nextToken()

	// Expect '=' or TO keyword
	if !(p.curTokenIs(TokenOperator) && p.curToken.Literal == "=") &&
		!(p.curTokenIs(TokenKeyword) && strings.EqualFold(p.curToken.Literal, "TO")) {
		p.addError(fmt.Sprintf("expected '=' or 'TO' after variable name at %s", p.curToken.Position))
		return nil
	}

	// Move past the '=' or 'TO'
	p.nextToken()

	// Parse the value
	stmt.Value = p.parseExpression(LOWEST)
	if stmt.Value == nil {
		p.addError(fmt.Sprintf("expected value for SET statement at %s", p.curToken.Position))
		return nil
	}

	return stmt
}

// parsePragmaStatement parses a PRAGMA statement for database settings
func (p *Parser) parsePragmaStatement() *PragmaStatement {
	// Create a new PRAGMA statement with the PRAGMA token
	stmt := &PragmaStatement{
		Token: p.curToken,
	}

	// Move past the PRAGMA token
	p.nextToken()

	// Parse the setting name
	if !p.curTokenIs(TokenIdentifier) {
		p.addError(fmt.Sprintf("expected identifier for pragma name at %s", p.curToken.Position))
		return nil
	}

	stmt.Name = &Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}

	// Move past the setting name
	p.nextToken()

	// Check if there's a value to set (optional)
	// PRAGMA can be used in two forms:
	// 1. PRAGMA setting_name (to get the current value)
	// 2. PRAGMA setting_name = value (to set a new value)
	if p.curTokenIs(TokenOperator) && p.curToken.Literal == "=" {
		// This is a PRAGMA assignment
		p.nextToken() // Move past the '='

		// Parse the value
		stmt.Value = p.parseExpression(LOWEST)
		if stmt.Value == nil {
			p.addError(fmt.Sprintf("expected value for PRAGMA statement at %s", p.curToken.Position))
			return nil
		}
	}

	return stmt
}
