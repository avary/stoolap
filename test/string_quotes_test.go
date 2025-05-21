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
)

func TestStringQuoteRemoval(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// The parser doesn't remove the quotes from the input string itself
		// It only removes them in its internal processing
		{"'simple string'", "simple string"},
		{"\"double quoted\"", "double quoted"},
		{"'2023-05-15'", "2023-05-15"},
		{"'14:30:00'", "14:30:00"},
	}

	for i, tt := range tests {
		l := parser.NewLexer(tt.input)
		// For each string, we'll just check how the lexer tokenizes it
		token := l.NextToken()

		// It should be a string token
		if token.Type != parser.TokenString {
			t.Fatalf("Test %d: Expected TokenString but got %v", i, token.Type)
		}

		// The literal should include the quotes
		if token.Literal != tt.input {
			t.Errorf("Test %d: Expected literal '%s' but got '%s'", i, tt.input, token.Literal)
		}

		// Now let's manually extract the string content (without quotes)
		// This simulates what the parser does
		content := ""
		if len(token.Literal) >= 2 {
			content = token.Literal[1 : len(token.Literal)-1]
		}

		if content != tt.expected {
			t.Errorf("Test %d: Expected content '%s' but got '%s'", i, tt.expected, content)
		}
	}
}
