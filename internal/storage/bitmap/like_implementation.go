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
package bitmap

import (
	"strings"
)

// GetMatchingLike returns a bitmap where the values match the SQL LIKE pattern
func (idx *Index) GetMatchingLike(likePattern string) (*Bitmap, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Create an empty result bitmap
	resultBitmap := New(idx.rowCount)

	// Check each value in the index
	for value, bitmap := range idx.valueMap {
		// For SQL standard compatibility, do case-insensitive comparison
		compareValue := strings.ToLower(value)
		pattern := strings.ToLower(likePattern)

		// Determine if there's a match
		var matches bool

		// In standard SQL, a pattern without wildcards is treated as an exact match
		if !strings.ContainsAny(pattern, "%_") {
			// No wildcards, use exact matching (standard SQL behavior)
			matches = compareValue == pattern
		} else {
			// Pattern has wildcards, use the pattern matching function
			matches = MatchSQLPattern(compareValue, pattern)
		}

		// If matches, set the corresponding bits in the result bitmap
		if matches {
			for i := int64(0); i < bitmap.Size(); i++ {
				val, _ := bitmap.Get(i)
				if val {
					resultBitmap.Set(i, true)
				}
			}
		}
	}

	return resultBitmap, nil
}
