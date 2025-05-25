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
package compression

import (
	"reflect"
	"testing"
)

func TestRunLengthEncoder(t *testing.T) {
	t.Run("CompressDecompress", func(t *testing.T) {
		encoder := NewRunLengthEncoder()

		// Test with repeated values
		values := []interface{}{
			"A", "A", "A", "A", "B", "B", "C", "D", "D", "D",
		}

		// Calculate raw size
		rawSize := 0
		for _, v := range values {
			rawSize += len(v.(string))
		}

		// Compress
		compressed, err := encoder.CompressValues(values)
		if err != nil {
			t.Fatalf("Error compressing: %v", err)
		}

		// Check compression ratio
		compressionRatio := float64(len(compressed)) / float64(rawSize)
		t.Logf("Compression ratio: %.2f", compressionRatio)

		// Decompress
		decompressed, err := encoder.DecompressValues(compressed)
		if err != nil {
			t.Fatalf("Error decompressing: %v", err)
		}

		// Verify values match
		if !reflect.DeepEqual(values, decompressed) {
			t.Errorf("Decompressed values don't match original:\nExpected: %v\nGot: %v", values, decompressed)
		}
	})

	t.Run("LongRuns", func(t *testing.T) {
		encoder := NewRunLengthEncoder()

		// Test with long runs of repeated values
		values := make([]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			if i < 500 {
				values[i] = "A"
			} else if i < 800 {
				values[i] = "B"
			} else {
				values[i] = "C"
			}
		}

		// Calculate raw size
		rawSize := 0
		for _, v := range values {
			rawSize += len(v.(string))
		}

		// Compress
		compressed, err := encoder.CompressValues(values)
		if err != nil {
			t.Fatalf("Error compressing: %v", err)
		}

		// Check compression ratio
		compressionRatio := float64(len(compressed)) / float64(rawSize)
		t.Logf("Compression ratio for long runs: %.4f", compressionRatio)
		t.Logf("Original size: %d bytes, Compressed size: %d bytes", rawSize, len(compressed))

		// Decompress
		decompressed, err := encoder.DecompressValues(compressed)
		if err != nil {
			t.Fatalf("Error decompressing: %v", err)
		}

		// Verify length
		if len(decompressed) != len(values) {
			t.Errorf("Decompressed length doesn't match original: %d vs %d",
				len(decompressed), len(values))
		}

		// Verify a few values
		for i := 0; i < len(values); i += 100 {
			if decompressed[i] != values[i] {
				t.Errorf("Value mismatch at index %d: expected %v, got %v",
					i, values[i], decompressed[i])
			}
		}
	})
}

func TestRunLengthCompressor(t *testing.T) {
	t.Run("CompressDecompress", func(t *testing.T) {
		compressor := NewRunLengthCompressor()

		// Create test data with repeated values
		testValues := []interface{}{
			"A", "A", "A", "A", "B", "B", "C", "D", "D", "D",
		}
		testData := encodeGenericValues(testValues)

		// Compress
		compressed, err := compressor.Compress(testData)
		if err != nil {
			t.Fatalf("Error compressing: %v", err)
		}

		// Check compression ratio
		compressionRatio := float64(len(compressed)) / float64(len(testData))
		t.Logf("Compression ratio: %.2f", compressionRatio)

		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Error decompressing: %v", err)
		}

		// Decode the decompressed data
		decodedValues, err := decodeGenericValues(decompressed)
		if err != nil {
			t.Fatalf("Error decoding decompressed data: %v", err)
		}

		// Verify values match
		if !reflect.DeepEqual(testValues, decodedValues) {
			t.Errorf("Decompressed values don't match original:\nExpected: %v\nGot: %v",
				testValues, decodedValues)
		}
	})

	t.Run("MixedTypes", func(t *testing.T) {
		compressor := NewRunLengthCompressor()

		// Create test data with mixed types and repeated values
		testValues := []interface{}{
			42, 42, 42, "hello", "hello", true, true, true, true, 3.14, 3.14,
		}
		testData := encodeGenericValues(testValues)

		// Compress
		compressed, err := compressor.Compress(testData)
		if err != nil {
			t.Fatalf("Error compressing: %v", err)
		}

		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Error decompressing: %v", err)
		}

		// Decode the decompressed data
		decodedValues, err := decodeGenericValues(decompressed)
		if err != nil {
			t.Fatalf("Error decoding decompressed data: %v", err)
		}

		// Verify length match
		if len(testValues) != len(decodedValues) {
			t.Errorf("Length mismatch: expected %d, got %d",
				len(testValues), len(decodedValues))
		}

		// Verify each value with appropriate type comparison
		for i, expected := range testValues {
			actual := decodedValues[i]

			switch expected := expected.(type) {
			case int:
				// For integers, the decoder might return int64
				actualInt, ok := actual.(int64)
				if !ok {
					t.Errorf("Type mismatch at index %d: expected int, got %T", i, actual)
					continue
				}
				if int(actualInt) != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v",
						i, expected, actualInt)
				}
			case string:
				actualStr, ok := actual.(string)
				if !ok {
					t.Errorf("Type mismatch at index %d: expected string, got %T", i, actual)
					continue
				}
				if actualStr != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v",
						i, expected, actualStr)
				}
			case bool:
				actualBool, ok := actual.(bool)
				if !ok {
					t.Errorf("Type mismatch at index %d: expected bool, got %T", i, actual)
					continue
				}
				if actualBool != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v",
						i, expected, actualBool)
				}
			case float64:
				actualFloat, ok := actual.(float64)
				if !ok {
					t.Errorf("Type mismatch at index %d: expected float64, got %T", i, actual)
					continue
				}
				if actualFloat != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v",
						i, expected, actualFloat)
				}
			}
		}
	})
}
