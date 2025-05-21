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
	"strings"
)

// Parse parses a SQL query and returns the parsed statement
func Parse(query string) (Statement, error) {
	// Normalize the query - replace potential problem cases with standard SQL
	query = strings.TrimSpace(query)

	// Remove any trailing semicolons to avoid parser errors
	query = strings.TrimSuffix(query, ";")

	l := NewLexer(query)
	p := NewParser(l)

	program := p.ParseProgram()

	if len(p.Errors()) > 0 {
		return nil, &SQLParseError{
			errors: p.Errors(),
		}
	}

	if len(program.Statements) == 0 {
		return nil, &SQLParseError{
			errors: []string{"No statements found in query"},
		}
	}

	// Return the first statement
	return program.Statements[0], nil
}

// SQLParseError represents a SQL parsing error
type SQLParseError struct {
	errors []string
}

// Error returns the error message
func (e *SQLParseError) Error() string {
	if len(e.errors) == 0 {
		return "SQL parse error"
	}
	return e.errors[0]
}

// Errors returns all parsing errors
func (e *SQLParseError) Errors() []string {
	return e.errors
}
