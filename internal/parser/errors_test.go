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
	"testing"
)

func TestUserFriendlyErrors(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectError   bool
		errorContains []string
	}{
		{
			name:        "Unclosed parenthesis",
			sql:         "SELECT * FROM users WHERE (id > 5",
			expectError: true,
			errorContains: []string{
				"expected next token to be PUNCTUATOR",
				"A punctuation character like",
			},
		},
		{
			name:        "Good query - no error",
			sql:         "SELECT id, name FROM users WHERE age > 18",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLexer(tt.sql)
			p := NewParser(l)

			// Parse and check for errors
			p.ParseProgram()
			hasErrors := len(p.Errors()) > 0

			if tt.expectError != hasErrors {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, hasErrors)
			}

			if !tt.expectError {
				return
			}

			// Format errors and check formatting
			formattedError := p.FormatErrors()
			if !strings.Contains(formattedError, "SQL parsing failed") {
				t.Errorf("Formatted error doesn't contain header: %s", formattedError)
			}

			// Check that the formatted error contains the original SQL
			if !strings.Contains(formattedError, tt.sql) {
				t.Errorf("Formatted error doesn't contain the original SQL: %s", formattedError)
			}

			// Verify error contains expected substrings
			for _, expected := range tt.errorContains {
				if !strings.Contains(formattedError, expected) {
					t.Errorf("Formatted error doesn't contain expected text '%s': %s", expected, formattedError)
				}
			}

			// Verify pointer indicator is present
			if !strings.Contains(formattedError, "^") {
				t.Errorf("Formatted error doesn't contain pointer indicator: %s", formattedError)
			}

			t.Logf("Formatted error (for visual inspection):\n%s", formattedError)
		})
	}
}
