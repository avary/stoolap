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
	"bytes"
	"encoding/binary"
	"fmt"
)

// RunLengthCompressor implements the Compressor interface for run-length encoding
type RunLengthCompressor struct {
	encoder *RunLengthEncoder
}

// NewRunLengthCompressor creates a new run-length compressor
func NewRunLengthCompressor() *RunLengthCompressor {
	return &RunLengthCompressor{
		encoder: NewRunLengthEncoder(),
	}
}

// Type returns the compression type
func (c *RunLengthCompressor) Type() CompressionType {
	return RunLength
}

// Compress compresses a byte slice containing data
func (c *RunLengthCompressor) Compress(data []byte) ([]byte, error) {
	// Decode the input data format
	values, err := decodeGenericValues(data)
	if err != nil {
		return nil, err
	}

	// Compress the values using the run-length encoder
	compressedData, err := c.encoder.CompressValues(values)
	if err != nil {
		return nil, err
	}

	return compressedData, nil
}

// Decompress decompresses run-length encoded data
func (c *RunLengthCompressor) Decompress(data []byte) ([]byte, error) {
	// Decompress the values
	values, err := c.encoder.DecompressValues(data)
	if err != nil {
		return nil, err
	}

	// Encode the values back to the original format
	return encodeGenericValues(values), nil
}

// decodeGenericValues decodes a byte slice into a slice of interface{} values
func decodeGenericValues(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return []interface{}{}, nil
	}

	buf := bytes.NewReader(data)

	// Read the number of values
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	values := make([]interface{}, count)

	// Read each value
	for i := uint32(0); i < count; i++ {
		// Read value type
		var typeCode uint8
		if err := binary.Read(buf, binary.LittleEndian, &typeCode); err != nil {
			return nil, err
		}

		// Read value based on type
		switch typeCode {
		case 0: // nil
			values[i] = nil
		case 1: // string
			var strLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}
			strData := make([]byte, strLen)
			if _, err := buf.Read(strData); err != nil {
				return nil, err
			}
			values[i] = string(strData)
		case 2: // int64
			var intVal int64
			if err := binary.Read(buf, binary.LittleEndian, &intVal); err != nil {
				return nil, err
			}
			values[i] = intVal
		case 3: // float64
			var floatVal float64
			if err := binary.Read(buf, binary.LittleEndian, &floatVal); err != nil {
				return nil, err
			}
			values[i] = floatVal
		case 4: // bool
			var boolVal uint8
			if err := binary.Read(buf, binary.LittleEndian, &boolVal); err != nil {
				return nil, err
			}
			values[i] = boolVal != 0
		default:
			return nil, fmt.Errorf("unknown type code: %d", typeCode)
		}
	}

	return values, nil
}

// encodeGenericValues encodes a slice of interface{} values into a byte slice
func encodeGenericValues(values []interface{}) []byte {
	var buf bytes.Buffer

	// Write the number of values
	count := uint32(len(values))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each value
	for _, v := range values {
		switch val := v.(type) {
		case nil:
			// Type code 0 for nil
			binary.Write(&buf, binary.LittleEndian, uint8(0))
		case string:
			// Type code 1 for string
			binary.Write(&buf, binary.LittleEndian, uint8(1))
			strLen := uint32(len(val))
			binary.Write(&buf, binary.LittleEndian, strLen)
			buf.Write([]byte(val))
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Type code 2 for integer
			binary.Write(&buf, binary.LittleEndian, uint8(2))
			// Convert to int64
			var intVal int64
			switch v := v.(type) {
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
			binary.Write(&buf, binary.LittleEndian, intVal)
		case float32, float64:
			// Type code 3 for float
			binary.Write(&buf, binary.LittleEndian, uint8(3))
			// Convert to float64
			var floatVal float64
			switch v := v.(type) {
			case float32:
				floatVal = float64(v)
			case float64:
				floatVal = v
			}
			binary.Write(&buf, binary.LittleEndian, floatVal)
		case bool:
			// Type code 4 for bool
			binary.Write(&buf, binary.LittleEndian, uint8(4))
			var boolVal uint8
			if val {
				boolVal = 1
			}
			binary.Write(&buf, binary.LittleEndian, boolVal)
		default:
			// Treat anything else as a string
			binary.Write(&buf, binary.LittleEndian, uint8(1))
			str := fmt.Sprintf("%v", val)
			strLen := uint32(len(str))
			binary.Write(&buf, binary.LittleEndian, strLen)
			buf.Write([]byte(str))
		}
	}

	return buf.Bytes()
}
