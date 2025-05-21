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

package mvcc

import (
	"math/rand"
	"sort"
	"testing"
)

// generateRandomSlice creates a slice of random int64 values
func generateRandomSlice(size int) []int64 {
	result := make([]int64, size)
	for i := 0; i < size; i++ {
		result[i] = rand.Int63()
	}
	return result
}

// isInt64SliceSorted checks if an int64 slice is sorted
func isInt64SliceSorted(a []int64) bool {
	for i := 1; i < len(a); i++ {
		if a[i] < a[i-1] {
			return false
		}
	}
	return true
}

// TestSIMDSortCorrectness verifies that our custom sort produces correctly sorted results
func TestSIMDSortCorrectness(t *testing.T) {
	// Test with various sizes
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		t.Run("Size"+string(rune(size)), func(t *testing.T) {
			// Generate random slice
			data := generateRandomSlice(size)

			// Make a copy for standard sort
			reference := make([]int64, len(data))
			copy(reference, data)

			// Sort with standard library
			sort.Slice(reference, func(i, j int) bool {
				return reference[i] < reference[j]
			})

			// Sort with our SIMD implementation
			SIMDSortInt64s(data)

			// Verify correctness
			if !isInt64SliceSorted(data) {
				t.Fatalf("SIMDSortInt64s failed to sort correctly")
			}

			// Compare with reference
			for i := 0; i < len(data); i++ {
				if data[i] != reference[i] {
					t.Fatalf("SIMDSortInt64s produced different result than sort.Slice at index %d", i)
				}
			}
		})
	}
}

// BenchmarkSIMDSortInt64s benchmarks our SIMD-optimized sort
func BenchmarkSIMDSortInt64s(b *testing.B) {
	benchSizes := []int{10, 100, 1000, 10000, 100000}

	for _, size := range benchSizes {
		b.Run("Size_"+string(rune(size)), func(b *testing.B) {
			data := generateRandomSlice(size)
			slices := make([][]int64, b.N)

			// Make copies to avoid affecting benchmark with sorted data
			for i := 0; i < b.N; i++ {
				slices[i] = make([]int64, len(data))
				copy(slices[i], data)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SIMDSortInt64s(slices[i])
			}
		})
	}
}

// BenchmarkSortSliceInt64s benchmarks the standard library sort for comparison
func BenchmarkSortSliceInt64s(b *testing.B) {
	benchSizes := []int{10, 100, 1000, 10000, 100000}

	for _, size := range benchSizes {
		b.Run("Size_"+string(rune(size)), func(b *testing.B) {
			data := generateRandomSlice(size)
			slices := make([][]int64, b.N)

			// Make copies to avoid affecting benchmark with sorted data
			for i := 0; i < b.N; i++ {
				slices[i] = make([]int64, len(data))
				copy(slices[i], data)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sort.Slice(slices[i], func(a, b int) bool {
					return slices[i][a] < slices[i][b]
				})
			}
		})
	}
}
