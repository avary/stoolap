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
// Package vectorized provides optimized vector operations for columnar data processing
package vectorized

import (
	"math"
)

// This file contains highly optimized versions of vector operations
// that benefit from compiler auto-vectorization on modern CPUs.
//
// These implementations can be replaced with platform-specific assembly
// versions on supported architectures (amd64, arm64) for maximum performance.
// The pure Go implementations here are designed to be auto-vectorized by
// the Go compiler on all platforms.

const (
	// Processing batch sizes for different operations
	smallBatchSize  = 8  // For simple operations
	mediumBatchSize = 16 // For medium complexity operations
	largeBatchSize  = 64 // For complex operations
)

//------------------------------------------------------------------------------
// Basic Vector Arithmetic Operations
//------------------------------------------------------------------------------

// MulFloat64SIMD multiplies two float64 arrays and stores result in dst
// Optimized for modern CPUs with SIMD instructions
func MulFloat64SIMD(dst, a, b []float64, length int) {
	// Process 8 elements at a time with manual loop unrolling
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] * b[i]
		dst[i+1] = a[i+1] * b[i+1]
		dst[i+2] = a[i+2] * b[i+2]
		dst[i+3] = a[i+3] * b[i+3]
		dst[i+4] = a[i+4] * b[i+4]
		dst[i+5] = a[i+5] * b[i+5]
		dst[i+6] = a[i+6] * b[i+6]
		dst[i+7] = a[i+7] * b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] * b[i]
	}
}

// AddFloat64SIMD adds two float64 arrays and stores result in dst
// Optimized for modern CPUs with SIMD instructions
func AddFloat64SIMD(dst, a, b []float64, length int) {
	// Process 8 elements at a time with manual loop unrolling
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] + b[i]
		dst[i+1] = a[i+1] + b[i+1]
		dst[i+2] = a[i+2] + b[i+2]
		dst[i+3] = a[i+3] + b[i+3]
		dst[i+4] = a[i+4] + b[i+4]
		dst[i+5] = a[i+5] + b[i+5]
		dst[i+6] = a[i+6] + b[i+6]
		dst[i+7] = a[i+7] + b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] + b[i]
	}
}

// SubFloat64SIMD subtracts array b from array a and stores result in dst
// Optimized for modern CPUs with SIMD instructions
func SubFloat64SIMD(dst, a, b []float64, length int) {
	// Process 8 elements at a time with manual loop unrolling
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] - b[i]
		dst[i+1] = a[i+1] - b[i+1]
		dst[i+2] = a[i+2] - b[i+2]
		dst[i+3] = a[i+3] - b[i+3]
		dst[i+4] = a[i+4] - b[i+4]
		dst[i+5] = a[i+5] - b[i+5]
		dst[i+6] = a[i+6] - b[i+6]
		dst[i+7] = a[i+7] - b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] - b[i]
	}
}

// DivFloat64SIMD divides array a by array b and stores result in dst
// Optimized for modern CPUs with SIMD instructions
func DivFloat64SIMD(dst, a, b []float64, length int) {
	// Process 8 elements at a time with manual loop unrolling
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] / b[i]
		dst[i+1] = a[i+1] / b[i+1]
		dst[i+2] = a[i+2] / b[i+2]
		dst[i+3] = a[i+3] / b[i+3]
		dst[i+4] = a[i+4] / b[i+4]
		dst[i+5] = a[i+5] / b[i+5]
		dst[i+6] = a[i+6] / b[i+6]
		dst[i+7] = a[i+7] / b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] / b[i]
	}
}

//------------------------------------------------------------------------------
// Scalar Operations (one array, one scalar value)
//------------------------------------------------------------------------------

// MulScalarFloat64SIMD multiplies each element in array by scalar value
func MulScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] * scalar
		dst[i+1] = a[i+1] * scalar
		dst[i+2] = a[i+2] * scalar
		dst[i+3] = a[i+3] * scalar
		dst[i+4] = a[i+4] * scalar
		dst[i+5] = a[i+5] * scalar
		dst[i+6] = a[i+6] * scalar
		dst[i+7] = a[i+7] * scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] * scalar
	}
}

// AddScalarFloat64SIMD adds scalar value to each element in array
func AddScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] + scalar
		dst[i+1] = a[i+1] + scalar
		dst[i+2] = a[i+2] + scalar
		dst[i+3] = a[i+3] + scalar
		dst[i+4] = a[i+4] + scalar
		dst[i+5] = a[i+5] + scalar
		dst[i+6] = a[i+6] + scalar
		dst[i+7] = a[i+7] + scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] + scalar
	}
}

// SubScalarFloat64SIMD subtracts a scalar from each element of an array: dst[i] = a[i] - scalar
func SubScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] - scalar
		dst[i+1] = a[i+1] - scalar
		dst[i+2] = a[i+2] - scalar
		dst[i+3] = a[i+3] - scalar
		dst[i+4] = a[i+4] - scalar
		dst[i+5] = a[i+5] - scalar
		dst[i+6] = a[i+6] - scalar
		dst[i+7] = a[i+7] - scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] - scalar
	}
}

// DivScalarFloat64SIMD divides each element of an array by a scalar: dst[i] = a[i] / scalar
func DivScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// For division, it's more efficient to multiply by the reciprocal
	reciprocal := 1.0 / scalar

	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] * reciprocal
		dst[i+1] = a[i+1] * reciprocal
		dst[i+2] = a[i+2] * reciprocal
		dst[i+3] = a[i+3] * reciprocal
		dst[i+4] = a[i+4] * reciprocal
		dst[i+5] = a[i+5] * reciprocal
		dst[i+6] = a[i+6] * reciprocal
		dst[i+7] = a[i+7] * reciprocal
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] * reciprocal
	}
}

// ReverseSubScalarFloat64SIMD calculates dst[i] = scalar - a[i]
func ReverseSubScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = scalar - a[i]
		dst[i+1] = scalar - a[i+1]
		dst[i+2] = scalar - a[i+2]
		dst[i+3] = scalar - a[i+3]
		dst[i+4] = scalar - a[i+4]
		dst[i+5] = scalar - a[i+5]
		dst[i+6] = scalar - a[i+6]
		dst[i+7] = scalar - a[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = scalar - a[i]
	}
}

// ReverseDivScalarFloat64SIMD calculates dst[i] = scalar / a[i]
func ReverseDivScalarFloat64SIMD(dst, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = scalar / a[i]
		dst[i+1] = scalar / a[i+1]
		dst[i+2] = scalar / a[i+2]
		dst[i+3] = scalar / a[i+3]
		dst[i+4] = scalar / a[i+4]
		dst[i+5] = scalar / a[i+5]
		dst[i+6] = scalar / a[i+6]
		dst[i+7] = scalar / a[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = scalar / a[i]
	}
}

//------------------------------------------------------------------------------
// Float64 Comparison Operations
//------------------------------------------------------------------------------

// CompareEQFloat64SIMD compares two float64 arrays element-wise for equality: dst[i] = a[i] == b[i]
func CompareEQFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] == b[i]
		dst[i+1] = a[i+1] == b[i+1]
		dst[i+2] = a[i+2] == b[i+2]
		dst[i+3] = a[i+3] == b[i+3]
		dst[i+4] = a[i+4] == b[i+4]
		dst[i+5] = a[i+5] == b[i+5]
		dst[i+6] = a[i+6] == b[i+6]
		dst[i+7] = a[i+7] == b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] == b[i]
	}
}

// CompareNEFloat64SIMD compares two float64 arrays element-wise for inequality: dst[i] = a[i] != b[i]
func CompareNEFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] != b[i]
		dst[i+1] = a[i+1] != b[i+1]
		dst[i+2] = a[i+2] != b[i+2]
		dst[i+3] = a[i+3] != b[i+3]
		dst[i+4] = a[i+4] != b[i+4]
		dst[i+5] = a[i+5] != b[i+5]
		dst[i+6] = a[i+6] != b[i+6]
		dst[i+7] = a[i+7] != b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] != b[i]
	}
}

// CompareGTFloat64SIMD compares two float64 arrays element-wise: dst[i] = a[i] > b[i]
func CompareGTFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] > b[i]
		dst[i+1] = a[i+1] > b[i+1]
		dst[i+2] = a[i+2] > b[i+2]
		dst[i+3] = a[i+3] > b[i+3]
		dst[i+4] = a[i+4] > b[i+4]
		dst[i+5] = a[i+5] > b[i+5]
		dst[i+6] = a[i+6] > b[i+6]
		dst[i+7] = a[i+7] > b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] > b[i]
	}
}

// CompareGEFloat64SIMD compares two float64 arrays element-wise: dst[i] = a[i] >= b[i]
func CompareGEFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] >= b[i]
		dst[i+1] = a[i+1] >= b[i+1]
		dst[i+2] = a[i+2] >= b[i+2]
		dst[i+3] = a[i+3] >= b[i+3]
		dst[i+4] = a[i+4] >= b[i+4]
		dst[i+5] = a[i+5] >= b[i+5]
		dst[i+6] = a[i+6] >= b[i+6]
		dst[i+7] = a[i+7] >= b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] >= b[i]
	}
}

// CompareLTFloat64SIMD compares two float64 arrays element-wise: dst[i] = a[i] < b[i]
func CompareLTFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] < b[i]
		dst[i+1] = a[i+1] < b[i+1]
		dst[i+2] = a[i+2] < b[i+2]
		dst[i+3] = a[i+3] < b[i+3]
		dst[i+4] = a[i+4] < b[i+4]
		dst[i+5] = a[i+5] < b[i+5]
		dst[i+6] = a[i+6] < b[i+6]
		dst[i+7] = a[i+7] < b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] < b[i]
	}
}

// CompareLEFloat64SIMD compares two float64 arrays element-wise: dst[i] = a[i] <= b[i]
func CompareLEFloat64SIMD(dst []bool, a, b []float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] <= b[i]
		dst[i+1] = a[i+1] <= b[i+1]
		dst[i+2] = a[i+2] <= b[i+2]
		dst[i+3] = a[i+3] <= b[i+3]
		dst[i+4] = a[i+4] <= b[i+4]
		dst[i+5] = a[i+5] <= b[i+5]
		dst[i+6] = a[i+6] <= b[i+6]
		dst[i+7] = a[i+7] <= b[i+7]
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] <= b[i]
	}
}

//------------------------------------------------------------------------------
// Float64 Scalar Comparison Operations
//------------------------------------------------------------------------------

// CompareEQScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] == scalar
func CompareEQScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] == scalar
		dst[i+1] = a[i+1] == scalar
		dst[i+2] = a[i+2] == scalar
		dst[i+3] = a[i+3] == scalar
		dst[i+4] = a[i+4] == scalar
		dst[i+5] = a[i+5] == scalar
		dst[i+6] = a[i+6] == scalar
		dst[i+7] = a[i+7] == scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] == scalar
	}
}

// CompareNEScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] != scalar
func CompareNEScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] != scalar
		dst[i+1] = a[i+1] != scalar
		dst[i+2] = a[i+2] != scalar
		dst[i+3] = a[i+3] != scalar
		dst[i+4] = a[i+4] != scalar
		dst[i+5] = a[i+5] != scalar
		dst[i+6] = a[i+6] != scalar
		dst[i+7] = a[i+7] != scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] != scalar
	}
}

// CompareGTScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] > scalar
func CompareGTScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] > scalar
		dst[i+1] = a[i+1] > scalar
		dst[i+2] = a[i+2] > scalar
		dst[i+3] = a[i+3] > scalar
		dst[i+4] = a[i+4] > scalar
		dst[i+5] = a[i+5] > scalar
		dst[i+6] = a[i+6] > scalar
		dst[i+7] = a[i+7] > scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] > scalar
	}
}

// CompareGEScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] >= scalar
func CompareGEScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] >= scalar
		dst[i+1] = a[i+1] >= scalar
		dst[i+2] = a[i+2] >= scalar
		dst[i+3] = a[i+3] >= scalar
		dst[i+4] = a[i+4] >= scalar
		dst[i+5] = a[i+5] >= scalar
		dst[i+6] = a[i+6] >= scalar
		dst[i+7] = a[i+7] >= scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] >= scalar
	}
}

// CompareLTScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] < scalar
func CompareLTScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] < scalar
		dst[i+1] = a[i+1] < scalar
		dst[i+2] = a[i+2] < scalar
		dst[i+3] = a[i+3] < scalar
		dst[i+4] = a[i+4] < scalar
		dst[i+5] = a[i+5] < scalar
		dst[i+6] = a[i+6] < scalar
		dst[i+7] = a[i+7] < scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] < scalar
	}
}

// CompareLEScalarFloat64SIMD compares a float64 array with a scalar: dst[i] = a[i] <= scalar
func CompareLEScalarFloat64SIMD(dst []bool, a []float64, scalar float64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] <= scalar
		dst[i+1] = a[i+1] <= scalar
		dst[i+2] = a[i+2] <= scalar
		dst[i+3] = a[i+3] <= scalar
		dst[i+4] = a[i+4] <= scalar
		dst[i+5] = a[i+5] <= scalar
		dst[i+6] = a[i+6] <= scalar
		dst[i+7] = a[i+7] <= scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] <= scalar
	}
}

//------------------------------------------------------------------------------
// Int64 Scalar Comparison Operations
//------------------------------------------------------------------------------

// CompareEQScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] == scalar
func CompareEQScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] == scalar
		dst[i+1] = a[i+1] == scalar
		dst[i+2] = a[i+2] == scalar
		dst[i+3] = a[i+3] == scalar
		dst[i+4] = a[i+4] == scalar
		dst[i+5] = a[i+5] == scalar
		dst[i+6] = a[i+6] == scalar
		dst[i+7] = a[i+7] == scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] == scalar
	}
}

// CompareNEScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] != scalar
func CompareNEScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] != scalar
		dst[i+1] = a[i+1] != scalar
		dst[i+2] = a[i+2] != scalar
		dst[i+3] = a[i+3] != scalar
		dst[i+4] = a[i+4] != scalar
		dst[i+5] = a[i+5] != scalar
		dst[i+6] = a[i+6] != scalar
		dst[i+7] = a[i+7] != scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] != scalar
	}
}

// CompareGTScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] > scalar
func CompareGTScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] > scalar
		dst[i+1] = a[i+1] > scalar
		dst[i+2] = a[i+2] > scalar
		dst[i+3] = a[i+3] > scalar
		dst[i+4] = a[i+4] > scalar
		dst[i+5] = a[i+5] > scalar
		dst[i+6] = a[i+6] > scalar
		dst[i+7] = a[i+7] > scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] > scalar
	}
}

// CompareGEScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] >= scalar
func CompareGEScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] >= scalar
		dst[i+1] = a[i+1] >= scalar
		dst[i+2] = a[i+2] >= scalar
		dst[i+3] = a[i+3] >= scalar
		dst[i+4] = a[i+4] >= scalar
		dst[i+5] = a[i+5] >= scalar
		dst[i+6] = a[i+6] >= scalar
		dst[i+7] = a[i+7] >= scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] >= scalar
	}
}

// CompareLTScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] < scalar
func CompareLTScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] < scalar
		dst[i+1] = a[i+1] < scalar
		dst[i+2] = a[i+2] < scalar
		dst[i+3] = a[i+3] < scalar
		dst[i+4] = a[i+4] < scalar
		dst[i+5] = a[i+5] < scalar
		dst[i+6] = a[i+6] < scalar
		dst[i+7] = a[i+7] < scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] < scalar
	}
}

// CompareLEScalarInt64SIMD compares an int64 array with a scalar: dst[i] = a[i] <= scalar
func CompareLEScalarInt64SIMD(dst []bool, a []int64, scalar int64, length int) {
	// Process 8 elements at a time
	i := 0
	for ; i <= length-8; i += 8 {
		dst[i] = a[i] <= scalar
		dst[i+1] = a[i+1] <= scalar
		dst[i+2] = a[i+2] <= scalar
		dst[i+3] = a[i+3] <= scalar
		dst[i+4] = a[i+4] <= scalar
		dst[i+5] = a[i+5] <= scalar
		dst[i+6] = a[i+6] <= scalar
		dst[i+7] = a[i+7] <= scalar
	}

	// Handle remaining elements
	for ; i < length; i++ {
		dst[i] = a[i] <= scalar
	}
}

//------------------------------------------------------------------------------
// Advanced Mathematical Operations
//------------------------------------------------------------------------------

// SqrtFloat64SIMD computes square root of each element in array
func SqrtFloat64SIMD(dst, a []float64, length int) {
	// Process in blocks to improve cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			dst[j] = math.Sqrt(a[j])
		}
	}
}

// ExpFloat64SIMD computes e^x for each element in array
func ExpFloat64SIMD(dst, a []float64, length int) {
	// Process in blocks to improve cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			dst[j] = math.Exp(a[j])
		}
	}
}

// LogFloat64SIMD computes natural logarithm of each element in array
func LogFloat64SIMD(dst, a []float64, length int) {
	// Process in blocks to improve cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			dst[j] = math.Log(a[j])
		}
	}
}

//------------------------------------------------------------------------------
// Statistical Functions
//------------------------------------------------------------------------------

// SumFloat64SIMD computes the sum of all elements in array
func SumFloat64SIMD(a []float64, length int) float64 {
	// Use multiple accumulators to increase instruction-level parallelism
	var sum1, sum2, sum3, sum4 float64

	// Process 4 elements at a time with multiple accumulators
	i := 0
	for ; i <= length-4; i += 4 {
		sum1 += a[i]
		sum2 += a[i+1]
		sum3 += a[i+2]
		sum4 += a[i+3]
	}

	// Handle remaining elements
	var sum5 float64
	for ; i < length; i++ {
		sum5 += a[i]
	}

	// Combine all accumulators
	return sum1 + sum2 + sum3 + sum4 + sum5
}

// MinMaxFloat64SIMD finds the minimum and maximum values in array
func MinMaxFloat64SIMD(a []float64, length int) (min, max float64) {
	if length == 0 {
		return 0, 0
	}

	// Initialize min and max with first element
	min = a[0]
	max = a[0]

	// Process 4 elements at a time
	i := 1 // Start from second element since we used the first one already
	for ; i <= length-4; i += 4 {
		// Check each element for min/max
		if a[i] < min {
			min = a[i]
		}
		if a[i] > max {
			max = a[i]
		}

		if a[i+1] < min {
			min = a[i+1]
		}
		if a[i+1] > max {
			max = a[i+1]
		}

		if a[i+2] < min {
			min = a[i+2]
		}
		if a[i+2] > max {
			max = a[i+2]
		}

		if a[i+3] < min {
			min = a[i+3]
		}
		if a[i+3] > max {
			max = a[i+3]
		}
	}

	// Handle remaining elements
	for ; i < length; i++ {
		if a[i] < min {
			min = a[i]
		}
		if a[i] > max {
			max = a[i]
		}
	}

	return min, max
}

// MeanFloat64SIMD computes the mean (average) of all elements in array
func MeanFloat64SIMD(a []float64, length int) float64 {
	if length == 0 {
		return 0
	}
	sum := SumFloat64SIMD(a, length)
	return sum / float64(length)
}

//------------------------------------------------------------------------------
// Boolean Operations
//------------------------------------------------------------------------------

// CompareEqual returns a bitmap of elements where a == b
func CompareEqual(a, b []float64, length int) []bool {
	result := make([]bool, length)

	// Process in blocks for better cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			result[j] = a[j] == b[j]
		}
	}

	return result
}

// CompareGreaterThan returns a bitmap of elements where a > b
func CompareGreaterThan(a, b []float64, length int) []bool {
	result := make([]bool, length)

	// Process in blocks for better cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			result[j] = a[j] > b[j]
		}
	}

	return result
}

// FilterByMask copies elements from src to dst where mask is true
// Returns the number of elements copied
func FilterByMask(dst, src []float64, mask []bool, length int) int {
	resultIdx := 0

	// Process in blocks for better cache utilization
	for i := 0; i < length; i += largeBatchSize {
		end := i + largeBatchSize
		if end > length {
			end = length
		}

		// Process current block
		for j := i; j < end; j++ {
			if mask[j] {
				dst[resultIdx] = src[j]
				resultIdx++
			}
		}
	}

	return resultIdx
}

//------------------------------------------------------------------------------
// Complex Functions and Formulas
//------------------------------------------------------------------------------

// OptimizedTrigFunction performs value * sin(angle) * cos(angle) * scale + offset calculation
// with optimized memory access patterns and computation order
func OptimizedTrigFunction(dst, values, scales, offsets []float64, angles []float64, length int) {
	for i := 0; i < length; i++ {
		// Fast angle normalization to 0-359 range
		angle := int(angles[i]) % 360
		if angle < 0 {
			angle += 360
		}

		// Get values from lookup tables
		sinVal := sinLookupTable[angle]
		cosVal := cosLookupTable[angle]

		// Minimize operations with careful ordering for better instruction pipelining
		tmp := sinVal * cosVal    // Compute once and reuse
		tmp = values[i] * tmp     // Multiply by value
		tmp = tmp * scales[i]     // Apply scaling
		dst[i] = tmp + offsets[i] // Add offset as final step
	}
}

// VectorizedTrigBatch does the same calculation as OptimizedTrigFunction
// but processes data in chunks for better memory locality
func VectorizedTrigBatch(result, values, scales, offsets, angles []float64, size int) {
	// Process in chunks that fit in L1 cache
	for start := 0; start < size; start += largeBatchSize {
		end := start + largeBatchSize
		if end > size {
			end = size
		}

		// Process current batch
		for i := start; i < end; i++ {
			// Fast angle normalization
			angle := int(angles[i]) % 360
			if angle < 0 {
				angle += 360
			}

			// Get values from lookup tables
			sinVal := sinLookupTable[angle]
			cosVal := cosLookupTable[angle]

			// Compute formula with minimal operations
			tmp := sinVal * cosVal
			tmp = values[i] * tmp
			tmp = tmp * scales[i]
			result[i] = tmp + offsets[i]
		}
	}
}

// Lookup tables for sin and cos values are defined in processor.go
// and initialized at package level
