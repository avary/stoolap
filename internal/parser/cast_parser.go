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
	"strings"
)

// parseCastExpression parses a CAST expression
func (p *Parser) parseCastExpression() Expression {
	// Save the current token (CAST keyword)
	castToken := p.curToken

	// Create a CAST expression with the current token
	expression := &CastExpression{
		Token: castToken,
	}

	// Expect opening parenthesis
	if !p.expectPeek(TokenPunctuator) || p.curToken.Literal != "(" {
		p.addError(fmt.Sprintf("expected '(' after CAST, got %s at %s", p.curToken.Literal, p.curToken.Position))
		return nil
	}

	// Move to the first token inside the parentheses
	p.nextToken()

	// Check for end of file
	if p.curTokenIs(TokenEOF) {
		p.addError(fmt.Sprintf("unexpected end of file in CAST expression at %s", p.curToken.Position))
		return nil
	}

	// Parse the expression to cast
	expression.Expr = p.parseExpression(LOWEST)
	if expression.Expr == nil {
		p.addError(fmt.Sprintf("expected expression in CAST at %s", p.curToken.Position))
		return nil
	}

	// Expect AS keyword
	if !p.expectPeek(TokenKeyword) || strings.ToUpper(p.curToken.Literal) != "AS" {
		p.addError(fmt.Sprintf("expected AS in CAST, got %s at %s", p.curToken.Literal, p.curToken.Position))
		return nil
	}

	// Parse the type name
	if p.peekTokenIs(TokenKeyword) {
		p.nextToken()
	} else if p.peekTokenIs(TokenIdentifier) {
		p.nextToken()
	} else {
		p.addError(fmt.Sprintf("expected type name after AS in CAST, got %s at %s", p.peekToken.Literal, p.peekToken.Position))
		return nil
	}

	// Store the type name
	expression.TypeName = p.curToken.Literal

	// Expect closing parenthesis
	if !p.expectPeek(TokenPunctuator) || p.curToken.Literal != ")" {
		p.addError(fmt.Sprintf("expected ')' after type name in CAST, got %s at %s", p.curToken.Literal, p.curToken.Position))
		return nil
	}

	return expression
}
