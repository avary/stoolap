package parser

import (
	"strings"
	"testing"
)

func TestErrorSuggestions(t *testing.T) {
	testCases := []struct {
		name        string
		sql         string
		shouldMatch []string
		skip        bool // Skip this test case
	}{
		{
			name: "missing table name",
			sql:  "SELECT * FROM",
			shouldMatch: []string{
				"missing a column or table name",
				"reserved keyword",
			},
		},
		{
			name: "typo in SELECT",
			sql:  "SELET * FROM users",
			shouldMatch: []string{
				"Did you mean 'SELECT'?",
			},
		},
		{
			name: "missing closing parenthesis",
			sql:  "SELECT * FROM users WHERE id IN (1, 2, 3",
			shouldMatch: []string{
				"closing parenthesis",
				"opening parentheses are matched",
			},
		},
		{
			name: "incorrect JOIN syntax",
			sql:  "SELECT * FROM users LEFTJOIN orders ON users.id = orders.user_id",
			shouldMatch: []string{
				"Did you mean 'LEFT JOIN'?",
				"needs a space between",
			},
		},
		{
			name: "PRAGMA syntax",
			sql:  "PRAGMA",
			shouldMatch: []string{
				"PRAGMA statements follow the format",
				"setting_name",
			},
		},
		{
			name: "missing ON in JOIN",
			sql:  "SELECT * FROM users JOIN orders",
			shouldMatch: []string{
				"JOIN clause is missing the ON condition",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL and expect errors
			result, err := ParseSQL(tc.sql)

			// We expect an error for these test cases
			if err == nil {
				t.Fatalf("Expected parsing error for '%s', but got success with: %v", tc.sql, result)
			}

			// Get formatted error messages
			errors := err.(*ParseErrors).Errors
			formattedErrors := FormatErrors(tc.sql, errors)

			// Check if error suggestions contain expected text
			for _, match := range tc.shouldMatch {
				if !strings.Contains(formattedErrors, match) {
					t.Errorf("Expected error suggestion to contain '%s', but got:\n%s", match, formattedErrors)
				}
			}
		})
	}
}
