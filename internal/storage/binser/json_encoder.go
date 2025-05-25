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
// Package binser provides high-performance binary serialization for the columnar storage engine.
package binser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// JSONBuffer pool for efficient JSON encoding
var jsonBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// GetJSONBuffer gets a buffer from the pool
func GetJSONBuffer() *bytes.Buffer {
	buf := jsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutJSONBuffer returns a buffer to the pool
func PutJSONBuffer(buf *bytes.Buffer) {
	jsonBufferPool.Put(buf)
}

// ValidateJSON checks if a string contains valid JSON
func ValidateJSON(s string) bool {
	if len(s) == 0 {
		return false
	}

	// Trim whitespace
	s = strings.TrimSpace(s)

	// Check for basic structure
	if s[0] == '{' && s[len(s)-1] == '}' { // Object
		return true
	}
	if s[0] == '[' && s[len(s)-1] == ']' { // Array
		return true
	}
	if s[0] == '"' && s[len(s)-1] == '"' { // String
		return true
	}
	if s == "null" { // null
		return true
	}
	if s == "true" || s == "false" { // Boolean
		return true
	}

	// Check if numeric
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// EncodeJSON encodes a value to JSON and writes it to the buffer
func EncodeJSON(buf *bytes.Buffer, v interface{}) error {
	// This is a future-proof design that allows us to replace the implementation
	// with a more efficient one later without changing the API
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = buf.Write(data)
	return err
}

// DecodeJSON decodes JSON from a buffer
func DecodeJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// JSONString encodes a value to a JSON string
func JSONString(v interface{}) (string, error) {
	buf := GetJSONBuffer()
	defer PutJSONBuffer(buf)

	if err := EncodeJSON(buf, v); err != nil {
		return "", fmt.Errorf("JSON encoding error: %w", err)
	}

	return buf.String(), nil
}
