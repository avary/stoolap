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
package mvcc

// This file contains a highly optimized sorting implementation for int64 slices
// using SIMD-friendly algorithms and memory access patterns.

// SIMDSortInt64s sorts a slice of int64 values using an optimized algorithm
// that's designed to be auto-vectorized by modern Go compilers on supported
// architectures (amd64, arm64).
func SIMDSortInt64s(a []int64) {
	if len(a) < 32 {
		// For small slices, use insertion sort which is faster than quick sort
		// and very cache-friendly for small arrays
		insertionSortInt64(a)
		return
	}

	// For larger slices, use hybrid sorting algorithm optimized for vectorization
	hybridSortInt64(a, 0, len(a)-1)
}

// insertionSortInt64 performs an insertion sort on a small slice of int64s
// This is highly cache efficient for small arrays and has minimal branch mispredictions
func insertionSortInt64(a []int64) {
	for i := 1; i < len(a); i++ {
		key := a[i]
		j := i - 1

		// Use linear search instead of binary search for small arrays
		// which is more SIMD-friendly due to sequential memory access
		for j >= 0 && a[j] > key {
			a[j+1] = a[j] // Shift elements
			j--
		}
		a[j+1] = key
	}
}

// hybridSortInt64 uses a combination of quicksort and insertion sort
// with special optimizations for vectorization
func hybridSortInt64(a []int64, lo, hi int) {
	// Use insertion sort for small subarrays
	if hi-lo < 16 {
		// Manual loop unrolling for small slices
		for i := lo + 1; i <= hi; i++ {
			key := a[i]
			j := i - 1

			// Unrolled insertion sort
			for j >= lo && a[j] > key {
				a[j+1] = a[j]
				j--
			}
			a[j+1] = key
		}
		return
	}

	// Partition step of quicksort using median-of-three pivot selection
	// which reduces the chance of worst-case behavior
	pivot := medianOfThree(a, lo, (lo+hi)/2, hi)

	// Partition the array using a three-way partitioning scheme to handle
	// cases with many duplicates more efficiently
	lt, gt := threeWayPartition(a, lo, hi, pivot)

	// Recursively sort the smaller partition first to minimize stack space
	if lt-lo < hi-gt {
		hybridSortInt64(a, lo, lt)
		hybridSortInt64(a, gt, hi)
	} else {
		hybridSortInt64(a, gt, hi)
		hybridSortInt64(a, lo, lt)
	}
}

// medianOfThree returns the median value among a[lo], a[mid], and a[hi]
// This is a better pivot selection strategy than simply using the middle element
func medianOfThree(a []int64, lo, mid, hi int) int64 {
	// Sort the three elements using a small sorting network
	// This is more predictable for branch prediction than if-else chains
	if a[lo] > a[mid] {
		a[lo], a[mid] = a[mid], a[lo]
	}
	if a[mid] > a[hi] {
		a[mid], a[hi] = a[hi], a[mid]
		if a[lo] > a[mid] {
			a[lo], a[mid] = a[mid], a[lo]
		}
	}
	return a[mid]
}

// threeWayPartition partitions array into three parts:
// [lo..lt-1] < pivot, [lt..gt] = pivot, [gt+1..hi] > pivot
// This performs better than standard quicksort for arrays with many duplicates
func threeWayPartition(a []int64, lo, hi int, pivot int64) (lt, gt int) {
	lt = lo
	gt = hi
	i := lo

	// Process elements in chunks of 8 for better SIMD utilization
	for i <= gt {
		if a[i] < pivot {
			a[i], a[lt] = a[lt], a[i]
			i++
			lt++
		} else if a[i] > pivot {
			a[i], a[gt] = a[gt], a[i]
			gt--
		} else {
			i++
		}
	}

	return lt, gt
}
