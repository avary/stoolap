package compression

import (
	"reflect"
	"testing"
)

func TestDictionaryEncoder(t *testing.T) {
	t.Run("AddValue", func(t *testing.T) {
		encoder := NewDictionaryEncoder()

		// Add values and check IDs
		id1 := encoder.AddValue("apple")
		if id1 != 0 {
			t.Errorf("Expected ID 0 for first value, got %d", id1)
		}

		id2 := encoder.AddValue("banana")
		if id2 != 1 {
			t.Errorf("Expected ID 1 for second value, got %d", id2)
		}

		// Add duplicate and check it gets the same ID
		id3 := encoder.AddValue("apple")
		if id3 != 0 {
			t.Errorf("Expected ID 0 for duplicate value, got %d", id3)
		}

		// Check cardinality
		if c := encoder.Cardinality(); c != 2 {
			t.Errorf("Expected cardinality 2, got %d", c)
		}
	})

	t.Run("GetID", func(t *testing.T) {
		encoder := NewDictionaryEncoder()
		encoder.AddValue("apple")
		encoder.AddValue("banana")

		// Check existing values
		id1, exists1 := encoder.GetID("apple")
		if !exists1 || id1 != 0 {
			t.Errorf("Expected ID 0 for 'apple', got %d (exists: %v)", id1, exists1)
		}

		// Check non-existing value
		_, exists2 := encoder.GetID("orange")
		if exists2 {
			t.Errorf("Expected 'orange' to not exist in dictionary")
		}
	})

	t.Run("GetValue", func(t *testing.T) {
		encoder := NewDictionaryEncoder()
		encoder.AddValue("apple")
		encoder.AddValue("banana")

		// Check existing IDs
		val1, err1 := encoder.GetValue(0)
		if err1 != nil || val1 != "apple" {
			t.Errorf("Expected value 'apple' for ID 0, got '%s' (err: %v)", val1, err1)
		}

		// Check non-existing ID
		_, err2 := encoder.GetValue(100)
		if err2 == nil {
			t.Errorf("Expected error for non-existing ID")
		}
	})

	t.Run("CompressDecompress", func(t *testing.T) {
		encoder := NewDictionaryEncoder()
		values := []string{"apple", "banana", "apple", "cherry", "banana", "date"}

		// Compress
		compressed, err := encoder.CompressStringBlock(values)
		if err != nil {
			t.Fatalf("Error compressing: %v", err)
		}

		// Check compression ratio
		compressionRatio := float64(len(compressed)) / float64(sumStringLengths(values))
		t.Logf("Compression ratio: %.2f", compressionRatio)

		// Decompress
		decompressed, err := encoder.DecompressStringBlock(compressed)
		if err != nil {
			t.Fatalf("Error decompressing: %v", err)
		}

		// Verify values match
		if !reflect.DeepEqual(values, decompressed) {
			t.Errorf("Decompressed values don't match original:\nExpected: %v\nGot: %v", values, decompressed)
		}
	})

	t.Run("SerializeDeserialize", func(t *testing.T) {
		encoder := NewDictionaryEncoder()

		// Add some values
		encoder.AddValue("apple")
		encoder.AddValue("banana")
		encoder.AddValue("cherry")

		// Serialize dictionary
		serialized, err := encoder.SerializeDictionary()
		if err != nil {
			t.Fatalf("Error serializing dictionary: %v", err)
		}

		// Create new encoder and deserialize
		newEncoder := NewDictionaryEncoder()
		err = newEncoder.DeserializeDictionary(serialized)
		if err != nil {
			t.Fatalf("Error deserializing dictionary: %v", err)
		}

		// Verify dictionaries match
		if encoder.Cardinality() != newEncoder.Cardinality() {
			t.Errorf("Cardinality mismatch: %d vs %d", encoder.Cardinality(), newEncoder.Cardinality())
		}

		// Check a few values
		id1, exists1 := newEncoder.GetID("apple")
		if !exists1 || id1 != 0 {
			t.Errorf("Expected ID 0 for 'apple', got %d (exists: %v)", id1, exists1)
		}

		val2, err2 := newEncoder.GetValue(1)
		if err2 != nil || val2 != "banana" {
			t.Errorf("Expected value 'banana' for ID 1, got '%s' (err: %v)", val2, err2)
		}
	})
}

func TestDictionaryCompressor(t *testing.T) {
	t.Run("CompressDecompress", func(t *testing.T) {
		compressor := NewDictionaryCompressor()

		// Create test data
		testStrings := []string{"apple", "banana", "apple", "cherry", "apple", "date", "banana"}
		testData := EncodeStringData(testStrings)

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
		decodedStrings, err := DecodeStringData(decompressed)
		if err != nil {
			t.Fatalf("Error decoding decompressed data: %v", err)
		}

		// Verify values match
		if !reflect.DeepEqual(testStrings, decodedStrings) {
			t.Errorf("Decompressed values don't match original:\nExpected: %v\nGot: %v", testStrings, decodedStrings)
		}
	})
}

// Helper function to sum the lengths of strings
func sumStringLengths(strings []string) int {
	sum := 0
	for _, s := range strings {
		sum += len(s)
	}
	return sum
}
