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
package vectorized

import (
	"math"
	"testing"
)

// TestSubScalarFloat64SIMD tests the SubScalarFloat64SIMD function
func TestSubScalarFloat64SIMD(t *testing.T) {
	const size = 100
	const scalar = 3.14159

	// Create test arrays
	a := make([]float64, size)
	dst := make([]float64, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i)
	}

	// Run SIMD function
	SubScalarFloat64SIMD(dst, a, scalar, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] - scalar
		if math.Abs(dst[i]-expected) > 1e-10 {
			t.Errorf("Index %d: expected %f, got %f", i, expected, dst[i])
		}
	}
}

// TestDivScalarFloat64SIMD tests the DivScalarFloat64SIMD function
func TestDivScalarFloat64SIMD(t *testing.T) {
	const size = 100
	const scalar = 3.14159

	// Create test arrays
	a := make([]float64, size)
	dst := make([]float64, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i + 1) // Avoid dividing by zero
	}

	// Run SIMD function
	DivScalarFloat64SIMD(dst, a, scalar, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] / scalar
		if math.Abs(dst[i]-expected) > 1e-10 {
			t.Errorf("Index %d: expected %f, got %f", i, expected, dst[i])
		}
	}
}

// TestReverseSubScalarFloat64SIMD tests the ReverseSubScalarFloat64SIMD function
func TestReverseSubScalarFloat64SIMD(t *testing.T) {
	const size = 100
	const scalar = 3.14159

	// Create test arrays
	a := make([]float64, size)
	dst := make([]float64, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i)
	}

	// Run SIMD function
	ReverseSubScalarFloat64SIMD(dst, a, scalar, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := scalar - a[i]
		if math.Abs(dst[i]-expected) > 1e-10 {
			t.Errorf("Index %d: expected %f, got %f", i, expected, dst[i])
		}
	}
}

// TestReverseDivScalarFloat64SIMD tests the ReverseDivScalarFloat64SIMD function
func TestReverseDivScalarFloat64SIMD(t *testing.T) {
	const size = 100
	const scalar = 3.14159

	// Create test arrays
	a := make([]float64, size)
	dst := make([]float64, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i + 1) // Avoid dividing by zero
	}

	// Run SIMD function
	ReverseDivScalarFloat64SIMD(dst, a, scalar, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := scalar / a[i]
		if math.Abs(dst[i]-expected) > 1e-10 {
			t.Errorf("Index %d: expected %f, got %f", i, expected, dst[i])
		}
	}
}

// TestCompareEQFloat64SIMD tests the CompareEQFloat64SIMD function
func TestCompareEQFloat64SIMD(t *testing.T) {
	const size = 100

	// Create test arrays
	a := make([]float64, size)
	b := make([]float64, size)
	dst := make([]bool, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i)
		// Make some values equal and some different
		if i%3 == 0 {
			b[i] = float64(i) // Equal
		} else {
			b[i] = float64(i + 1) // Different
		}
	}

	// Run SIMD function
	CompareEQFloat64SIMD(dst, a, b, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] == b[i]
		if dst[i] != expected {
			t.Errorf("Index %d: expected %v, got %v", i, expected, dst[i])
		}
	}
}

// TestCompareNEFloat64SIMD tests the CompareNEFloat64SIMD function
func TestCompareNEFloat64SIMD(t *testing.T) {
	const size = 100

	// Create test arrays
	a := make([]float64, size)
	b := make([]float64, size)
	dst := make([]bool, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i)
		// Make some values equal and some different
		if i%3 == 0 {
			b[i] = float64(i) // Equal
		} else {
			b[i] = float64(i + 1) // Different
		}
	}

	// Run SIMD function
	CompareNEFloat64SIMD(dst, a, b, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] != b[i]
		if dst[i] != expected {
			t.Errorf("Index %d: expected %v, got %v", i, expected, dst[i])
		}
	}
}

// TestCompareGTFloat64SIMD tests the CompareGTFloat64SIMD function
func TestCompareGTFloat64SIMD(t *testing.T) {
	const size = 100

	// Create test arrays
	a := make([]float64, size)
	b := make([]float64, size)
	dst := make([]bool, size)

	// Fill with test data
	for i := 0; i < size; i++ {
		a[i] = float64(i)
		b[i] = float64(i - (i % 3)) // This creates a mix of a>b, a==b, and a<b
	}

	// Run SIMD function
	CompareGTFloat64SIMD(dst, a, b, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] > b[i]
		if dst[i] != expected {
			t.Errorf("Index %d: expected %v, got %v", i, expected, dst[i])
		}
	}
}

// TestEdgeCases tests edge cases for our SIMD functions
func TestEdgeCases(t *testing.T) {
	t.Run("SmallArraySizes", func(t *testing.T) {
		// Test with array sizes that aren't multiples of 8 to test remainder handling
		for _, size := range []int{1, 3, 7, 9, 15} {
			a := make([]float64, size)
			b := make([]float64, size)
			dst := make([]float64, size)
			boolDst := make([]bool, size)

			for i := 0; i < size; i++ {
				a[i] = float64(i + 1)
				b[i] = float64(i + 2)
			}

			// Test subtraction with scalar
			SubScalarFloat64SIMD(dst, a, 2.5, size)
			for i := 0; i < size; i++ {
				expected := a[i] - 2.5
				if math.Abs(dst[i]-expected) > 1e-10 {
					t.Errorf("SubScalarFloat64SIMD remainder handling index %d: expected %f, got %f",
						i, expected, dst[i])
				}
			}

			// Test boolean comparison
			CompareGTFloat64SIMD(boolDst, a, b, size)
			for i := 0; i < size; i++ {
				expected := a[i] > b[i]
				if boolDst[i] != expected {
					t.Errorf("CompareGTFloat64SIMD remainder handling index %d: expected %v, got %v",
						i, expected, boolDst[i])
				}
			}
		}
	})
}

// TestCompareEQScalarFloat64SIMD tests the CompareEQScalarFloat64SIMD function
func TestCompareEQScalarFloat64SIMD(t *testing.T) {
	const size = 100
	const scalar = 42.0

	// Create test arrays
	a := make([]float64, size)
	dst := make([]bool, size)

	// Fill with test data - make some match the scalar
	for i := 0; i < size; i++ {
		if i%5 == 0 {
			a[i] = scalar // Should match
		} else {
			a[i] = float64(i) // Should not match
		}
	}

	// Run SIMD function
	CompareEQScalarFloat64SIMD(dst, a, scalar, size)

	// Verify results
	for i := 0; i < size; i++ {
		expected := a[i] == scalar
		if dst[i] != expected {
			t.Errorf("Index %d: expected %v, got %v", i, expected, dst[i])
		}
	}
}
