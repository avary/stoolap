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
