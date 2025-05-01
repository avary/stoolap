package test

import (
	"testing"

	"github.com/semihalev/stoolap/internal/storage"
)

func TestJSONParsing(t *testing.T) {
	// Test parsing JSON through the storage package functions
	// This doesn't rely on the SQL parser, just on the storage conversion functions

	// Test with a simple JSON object
	jsonStr := `{"name":"John","age":30}`
	result := storage.ConvertStorageValueToGoValue(jsonStr, storage.JSON)

	// With our current implementation, we're just returning the string as is
	if result != jsonStr {
		t.Errorf("Expected JSON string to be returned as is, got %v", result)
	}

	// Test with a JSON array
	jsonArray := `[1,2,3,4]`
	result = storage.ConvertStorageValueToGoValue(jsonArray, storage.JSON)

	if result != jsonArray {
		t.Errorf("Expected JSON array to be returned as is, got %v", result)
	}

	// Test with a map (already parsed JSON)
	jsonMap := map[string]interface{}{
		"name": "John",
		"age":  30,
	}
	result = storage.ConvertStorageValueToGoValue(jsonMap, storage.JSON)

	// Since maps can't be directly compared in Go, we need to compare their contents
	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Errorf("Expected map but got %T", result)
		return
	}

	// Check map size
	if len(jsonMap) != len(resultMap) {
		t.Errorf("Expected map of size %d but got %d", len(jsonMap), len(resultMap))
		return
	}

	// Check each key-value pair
	for k, v := range jsonMap {
		if resultVal, exists := resultMap[k]; exists {
			// For simplicity, just check if the string representation matches
			if resultVal != v {
				t.Errorf("For key %s, expected %v but got %v", k, v, resultVal)
			}
		} else {
			t.Errorf("Key %s not found in result map", k)
		}
	}
}
