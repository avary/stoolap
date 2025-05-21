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

package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// RunLengthEncoder implements run-length encoding for all column types
// This is a simple implementation that works for any type as long as
// you can compare values for equality
type RunLengthEncoder struct {
	// Current state
	compressed bool
}

// NewRunLengthEncoder creates a new run-length encoder
func NewRunLengthEncoder() *RunLengthEncoder {
	return &RunLengthEncoder{
		compressed: false,
	}
}

// CompressValues compresses a slice of any type using run-length encoding
func (r *RunLengthEncoder) CompressValues(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Estimate buffer size
	// We have total value count (4 bytes) +
	// for each run: type (1 byte) + value (varies) + count (4 bytes)
	// Average case might be ~10% of values being run starts
	estimatedRuns := 1 + (len(values) / 10)
	estimatedSize := 4 + (estimatedRuns * 10)
	buf := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// Write the total count of values
	valueCount := uint32(len(values))
	binary.Write(buf, binary.LittleEndian, valueCount)

	// Compress the values using run-length encoding
	var run struct {
		value interface{}
		count uint32
		typ   uint8 // Type code for the value
	}

	run.value = values[0]
	run.count = 1
	run.typ = getTypeCode(run.value)

	// Optimization: use string interning for repeated string comparisons
	// by caching the string representation of the current run value
	var currentStr string
	if run.typ == 1 { // string
		currentStr = run.value.(string)
	} else {
		currentStr = fmt.Sprintf("%v", run.value)
	}

	// Process the remaining values
	for i := 1; i < len(values); i++ {
		nextTyp := getTypeCode(values[i])

		// Fast path string comparison for common case
		if nextTyp == 1 && run.typ == 1 {
			// Direct string comparison
			if values[i].(string) == run.value.(string) {
				run.count++
				continue
			}
		} else if nextTyp == run.typ {
			// For other types, compare string representations
			var nextStr string
			if nextTyp == 1 { // string
				nextStr = values[i].(string)
			} else {
				nextStr = fmt.Sprintf("%v", values[i])
			}

			if nextStr == currentStr {
				run.count++
				continue
			}
		}

		// Different value, write the current run
		writeRun(buf, run.typ, run.value, run.count)

		// Start a new run
		run.value = values[i]
		run.count = 1
		run.typ = nextTyp

		// Update cached string representation
		if run.typ == 1 { // string
			currentStr = run.value.(string)
		} else {
			currentStr = fmt.Sprintf("%v", run.value)
		}
	}

	// Write the final run
	writeRun(buf, run.typ, run.value, run.count)

	r.compressed = true
	return buf.Bytes(), nil
}

// writeRun is a helper function to write a single run to the buffer
func writeRun(buf *bytes.Buffer, typ uint8, value interface{}, count uint32) error {
	// Use variable byte encoding for run length to save space
	// 1-byte encoding for counts 0-127
	// 2-byte encoding for counts 128-16383
	// 4-byte encoding for larger counts

	// First check if we can use a compact encoding
	if count <= 127 {
		// 1-byte encoding (bit pattern: 0xxxxxxx)
		typ = typ | 0x00 // No high bit flags
		binary.Write(buf, binary.LittleEndian, typ)
		buf.WriteByte(byte(count))
	} else if count <= 16383 {
		// 2-byte encoding (bit pattern: 10xxxxxx xxxxxxxx)
		typ = typ | 0x80 // Set high bit for 2-byte encoding
		binary.Write(buf, binary.LittleEndian, typ)

		// Split into two bytes
		highByte := byte((count >> 8) & 0x3F) // 6 bits
		lowByte := byte(count & 0xFF)         // 8 bits
		buf.WriteByte(highByte)
		buf.WriteByte(lowByte)
	} else {
		// 4-byte encoding (bit pattern: 11xxxxxx + 3 bytes)
		typ = typ | 0xC0 // Set two high bits for 4-byte encoding
		binary.Write(buf, binary.LittleEndian, typ)

		// Write 3 bytes (we only need 24 bits for run lengths up to 16M)
		buf.WriteByte(byte((count >> 16) & 0xFF))
		buf.WriteByte(byte((count >> 8) & 0xFF))
		buf.WriteByte(byte(count & 0xFF))
	}

	// Write value based on type (lower 4 bits of typ)
	valueType := typ & 0x0F
	switch valueType {
	case 0: // nil
		// No value to write
	case 1: // string
		str := value.(string)
		strLen := uint32(len(str))
		binary.Write(buf, binary.LittleEndian, strLen)
		buf.Write([]byte(str))
	case 2: // int
		var intVal int64
		switch v := value.(type) {
		case int:
			intVal = int64(v)
		case int8:
			intVal = int64(v)
		case int16:
			intVal = int64(v)
		case int32:
			intVal = int64(v)
		case int64:
			intVal = v
		case uint:
			intVal = int64(v)
		case uint8:
			intVal = int64(v)
		case uint16:
			intVal = int64(v)
		case uint32:
			intVal = int64(v)
		case uint64:
			intVal = int64(v)
		}
		binary.Write(buf, binary.LittleEndian, intVal)
	case 3: // float
		var floatVal float64
		switch v := value.(type) {
		case float32:
			floatVal = float64(v)
		case float64:
			floatVal = v
		}
		binary.Write(buf, binary.LittleEndian, floatVal)
	case 4: // bool
		var boolVal uint8
		if value.(bool) {
			boolVal = 1
		}
		binary.Write(buf, binary.LittleEndian, boolVal)
	default:
		// For unknown types, convert to string
		str := fmt.Sprintf("%v", value)
		strLen := uint32(len(str))
		binary.Write(buf, binary.LittleEndian, strLen)
		buf.Write([]byte(str))
	}

	return nil
}

// getTypeCode returns the type code for a value
func getTypeCode(v interface{}) uint8 {
	switch v.(type) {
	case nil:
		return 0
	case string:
		return 1
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 2
	case float32, float64:
		return 3
	case bool:
		return 4
	default:
		return 1 // Treat unknown types as strings
	}
}

// DecompressValues decompresses data that was compressed with run-length encoding
func (r *RunLengthEncoder) DecompressValues(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return []interface{}{}, nil
	}

	buf := bytes.NewReader(data)

	// Read the total count of values
	var valueCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
		return nil, err
	}

	// Pre-allocate result slice with capacity for all values
	result := make([]interface{}, 0, valueCount)

	// Read runs until we reach the total count
	for len(result) < int(valueCount) && buf.Len() > 0 {
		// Read combined type+encoding byte
		var typeByte uint8
		if err := binary.Read(buf, binary.LittleEndian, &typeByte); err != nil {
			return nil, err
		}

		// Extract the type code (lower 4 bits)
		typeCode := typeByte & 0x0F

		// Extract run length based on encoding format
		var count uint32
		if (typeByte & 0x80) == 0 {
			// 1-byte encoding (0xxxxxxx)
			// ReadByte returns two values, but we need to check for errors
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			count = uint32(b)

			// Unread the byte so we can read it properly
			if err := buf.UnreadByte(); err != nil {
				return nil, err
			}

			// Read the byte
			var countByte byte
			if err := binary.Read(buf, binary.LittleEndian, &countByte); err != nil {
				return nil, err
			}
			count = uint32(countByte)
		} else if (typeByte & 0xC0) == 0x80 {
			// 2-byte encoding (10xxxxxx xxxxxxxx)
			// Read the two bytes
			var highByte, lowByte byte
			if err := binary.Read(buf, binary.LittleEndian, &highByte); err != nil {
				return nil, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &lowByte); err != nil {
				return nil, err
			}

			// Combine them
			highPart := uint32(highByte&0x3F) << 8
			lowPart := uint32(lowByte)
			count = highPart | lowPart
		} else {
			// 4-byte encoding (11xxxxxx + 3 more bytes)
			// Read the three bytes
			var b1, b2, b3 byte
			if err := binary.Read(buf, binary.LittleEndian, &b1); err != nil {
				return nil, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &b2); err != nil {
				return nil, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &b3); err != nil {
				return nil, err
			}

			// Combine them
			count = (uint32(b1) << 16) | (uint32(b2) << 8) | uint32(b3)
		}

		// Read value based on type
		var value interface{}

		switch typeCode {
		case 0: // nil
			value = nil
		case 1: // string
			var strLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}

			valueBytes := make([]byte, strLen)
			if _, err := buf.Read(valueBytes); err != nil {
				return nil, err
			}
			value = string(valueBytes)
		case 2: // int
			var intVal int64
			if err := binary.Read(buf, binary.LittleEndian, &intVal); err != nil {
				return nil, err
			}
			value = intVal
		case 3: // float
			var floatVal float64
			if err := binary.Read(buf, binary.LittleEndian, &floatVal); err != nil {
				return nil, err
			}
			value = floatVal
		case 4: // bool
			var boolVal uint8
			if err := binary.Read(buf, binary.LittleEndian, &boolVal); err != nil {
				return nil, err
			}
			value = boolVal != 0
		default:
			// For unknown types, treat as string
			var strLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}

			valueBytes := make([]byte, strLen)
			if _, err := buf.Read(valueBytes); err != nil {
				return nil, err
			}
			value = string(valueBytes)
		}

		// Optimization: For large runs, avoid individual appends
		if count > 100 {
			// Create a slice of the same value repeated count times
			for i := uint32(0); i < count; i++ {
				result = append(result, value)
			}
		} else {
			// For smaller runs, just append in a loop
			for i := uint32(0); i < count; i++ {
				result = append(result, value)
			}
		}
	}

	if len(result) != int(valueCount) {
		return nil, fmt.Errorf("expected %d values, got %d", valueCount, len(result))
	}

	return result, nil
}
