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

package bitmap

import "strings"

// MatchSQLPattern checks if a string matches an SQL LIKE pattern
// Pattern can contain:
// - '%' to match any sequence of characters (including empty)
// - '_' to match exactly one character
func MatchSQLPattern(str, pattern string) bool {
	// Standard implementation using the backtracking method
	// which is a common algorithm for SQL LIKE pattern matching

	// Case-insensitive comparison (SQL standard)
	strLower := strings.ToLower(str)
	patternLower := strings.ToLower(pattern)

	// Use an iterative backtracking approach that's more efficient
	// for SQL pattern matching and works correctly with wildcard combinations
	return matchPattern(strLower, patternLower)
}

// matchPattern implements the actual pattern matching algorithm
// using an iterative approach with backtracking for SQL LIKE patterns
func matchPattern(str, pattern string) bool {
	// Special cases
	if pattern == "" {
		return str == ""
	}

	// Start indices for backtracking
	var sp, pp int = 0, 0

	// Indices for potential star match
	var starIdx, strBacktrack int = -1, -1

	// Iterate through the string
	for sp < len(str) {
		// If we have more pattern to match and the current chars match
		// (either direct match or '_' wildcard)
		if pp < len(pattern) && (pattern[pp] == '_' || pattern[pp] == str[sp]) {
			// Move to next char in both strings
			sp++
			pp++
		} else if pp < len(pattern) && pattern[pp] == '%' {
			// Found a '%' wildcard - mark position for backtracking
			starIdx = pp
			strBacktrack = sp

			// Move to next pattern char (% can match 0 chars)
			pp++
		} else if starIdx != -1 {
			// No direct match, but we have a previous '%' to backtrack to
			// Reset pattern position to just after the star
			pp = starIdx + 1

			// Move string position forward by 1 from last backtrack point
			// (% matches one more char)
			strBacktrack++
			sp = strBacktrack
		} else {
			// No match and no backtracking point
			return false
		}
	}

	// Skip any remaining '%' wildcards in pattern
	for pp < len(pattern) && pattern[pp] == '%' {
		pp++
	}

	// Match if we've used the entire pattern
	return pp == len(pattern)
}
