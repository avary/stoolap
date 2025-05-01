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
