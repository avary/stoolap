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
package binser

import (
	"fmt"
	"os"
	"path/filepath"
)

// BlockIO provides fast serialization and deserialization for data blocks.
// This is optimized for the columnar storage engine's block operations.

// SaveBlock saves a data block to disk with optimized binary serialization
func SaveBlock(blockData []byte, filePath string) error {
	// Create parent directory if needed
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}

	// Write atomically to avoid partial writes
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, blockData, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, filePath)
}

// LoadBlock loads a data block from disk
func LoadBlock(filePath string) ([]byte, error) {
	return os.ReadFile(filePath)
}

// StringSliceExtensions provides optimized serialization for string slices,
// which are commonly used for column names, etc.

// WriteStringSlice writes a string slice to the writer
func WriteStringSlice(w *Writer, strings []string) {
	w.WriteArrayHeader(len(strings))
	for _, s := range strings {
		w.WriteString(s)
	}
}

// ReadStringSlice reads a string slice from the reader
func ReadStringSlice(r *Reader) ([]string, error) {
	length, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}

	result := make([]string, length)
	for i := 0; i < length; i++ {
		result[i], err = r.ReadString()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// IntSliceExtensions provides optimized serialization for int slices,
// which are commonly used for row IDs, positions, etc.

// WriteIntSlice writes an int slice to the writer
// It automatically uses the smallest possible integer type
func WriteIntSlice(w *Writer, ints []int) {
	w.WriteArrayHeader(len(ints))

	// Determine the range of values to choose optimal encoding
	min, max := 0, 0
	if len(ints) > 0 {
		min, max = ints[0], ints[0]
		for _, v := range ints {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}

	// Choose optimal encoding based on range
	if min >= 0 && max <= 255 {
		// uint8 encoding
		w.WriteUint8(1) // encoding type
		for _, v := range ints {
			w.WriteUint8(uint8(v))
		}
	} else if min >= -128 && max <= 127 {
		// int8 encoding
		w.WriteUint8(2) // encoding type
		for _, v := range ints {
			w.WriteInt8(int8(v))
		}
	} else if min >= 0 && max <= 65535 {
		// uint16 encoding
		w.WriteUint8(3) // encoding type
		for _, v := range ints {
			w.WriteUint16(uint16(v))
		}
	} else if min >= -32768 && max <= 32767 {
		// int16 encoding
		w.WriteUint8(4) // encoding type
		for _, v := range ints {
			w.WriteInt16(int16(v))
		}
	} else if min >= 0 && max <= 4294967295 {
		// uint32 encoding
		w.WriteUint8(5) // encoding type
		for _, v := range ints {
			w.WriteUint32(uint32(v))
		}
	} else if min >= -2147483648 && max <= 2147483647 {
		// int32 encoding
		w.WriteUint8(6) // encoding type
		for _, v := range ints {
			w.WriteInt32(int32(v))
		}
	} else {
		// int64 encoding
		w.WriteUint8(7) // encoding type
		for _, v := range ints {
			w.WriteInt64(int64(v))
		}
	}
}

// ReadIntSlice reads an int slice from the reader
func ReadIntSlice(r *Reader) ([]int, error) {
	length, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}

	result := make([]int, length)

	// Read encoding type
	encodingType, err := r.ReadUint8()
	if err != nil {
		return nil, err
	}

	// Decode based on encoding type
	switch encodingType {
	case 1: // uint8
		for i := 0; i < length; i++ {
			val, err := r.ReadUint8()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 2: // int8
		for i := 0; i < length; i++ {
			val, err := r.ReadInt8()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 3: // uint16
		for i := 0; i < length; i++ {
			val, err := r.ReadUint16()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 4: // int16
		for i := 0; i < length; i++ {
			val, err := r.ReadInt16()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 5: // uint32
		for i := 0; i < length; i++ {
			val, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 6: // int32
		for i := 0; i < length; i++ {
			val, err := r.ReadInt32()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	case 7: // int64
		for i := 0; i < length; i++ {
			val, err := r.ReadInt64()
			if err != nil {
				return nil, err
			}
			result[i] = int(val)
		}
	default:
		return nil, ErrInvalidType
	}

	return result, nil
}

// BlockMetadata extensions provide serialization for block metadata
// which is important for efficient zone map filtering and block management

// WriteBlockZoneMap writes zone map metadata to the writer
func WriteBlockZoneMap(w *Writer, minValue, maxValue interface{}) {
	// Determine if we have null values
	if minValue == nil && maxValue == nil {
		w.WriteNull()
		return
	}

	// If min/max values are different types, handle as strings
	if minValue != nil && maxValue != nil {
		switch minValue.(type) {
		case int, int8, int16, int32, int64:
			if _, ok := maxValue.(int64); !ok {
				// Serialize as strings
				w.WriteString(toString(minValue))
				w.WriteString(toString(maxValue))
				return
			}
			// Proceed with numeric encoding
		case float32, float64:
			if _, ok := maxValue.(float64); !ok {
				// Serialize as strings
				w.WriteString(toString(minValue))
				w.WriteString(toString(maxValue))
				return
			}
			// Proceed with numeric encoding
		default:
			// Just serialize as strings
			w.WriteString(toString(minValue))
			w.WriteString(toString(maxValue))
			return
		}
	}

	// Encode based on type
	switch min := minValue.(type) {
	case int:
		w.WriteInt32(int32(min))
		w.WriteInt32(int32(maxValue.(int)))
	case int64:
		w.WriteInt64(min)
		w.WriteInt64(maxValue.(int64))
	case float32:
		w.WriteFloat32(min)
		w.WriteFloat32(maxValue.(float32))
	case float64:
		w.WriteFloat64(min)
		w.WriteFloat64(maxValue.(float64))
	case string:
		w.WriteString(min)
		w.WriteString(maxValue.(string))
	default:
		// Just serialize as strings for any other type
		w.WriteString(toString(minValue))
		w.WriteString(toString(maxValue))
	}
}

// Helper to convert any value to a string
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
