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
	"errors"
	"fmt"
)

// BitPackCompressor implements the Compressor interface for bit packing
type BitPackCompressor struct {
	encoder *BitPackEncoder
}

// NewBitPackCompressor creates a new bit pack compressor
func NewBitPackCompressor() *BitPackCompressor {
	return &BitPackCompressor{
		encoder: NewBitPackEncoder(),
	}
}

// Type returns the compression type
func (c *BitPackCompressor) Type() CompressionType {
	return BitPack
}

// Compress compresses boolean values using bit packing
func (c *BitPackCompressor) Compress(data []byte) ([]byte, error) {
	// Decode the booleans from the input byte slice
	boolValues, err := decodeBooleanData(data)
	if err != nil {
		return nil, err
	}

	// Compress using bit packing
	packedData, err := c.encoder.Encode(boolValues)
	if err != nil {
		return nil, err
	}

	// Create the result buffer with header
	result := make([]byte, CompressionHeaderSize+len(packedData))

	// Set compression type
	result[0] = byte(c.Type())

	// Set original data size
	binary.LittleEndian.PutUint32(result[1:CompressionHeaderSize], uint32(len(data)))

	// Copy packed data
	copy(result[CompressionHeaderSize:], packedData)

	return result, nil
}

// Decompress decompresses bit-packed data
func (c *BitPackCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) < CompressionHeaderSize {
		return nil, errors.New("invalid compressed data: header too small")
	}

	// Verify compression type
	compressionType := CompressionType(data[0])
	if compressionType != c.Type() {
		return nil, fmt.Errorf("invalid compression type: expected %d, got %d", c.Type(), compressionType)
	}

	// Get the original data size
	originalSize := binary.LittleEndian.Uint32(data[1:CompressionHeaderSize])

	// Skip the header and decompress
	compressedData := data[CompressionHeaderSize:]
	if len(compressedData) == 0 {
		return []byte{}, nil
	}

	// Decode the bit-packed data to boolean values
	boolValues, err := c.encoder.Decode(compressedData)
	if err != nil {
		return nil, err
	}

	// Encode boolean values back to the original format
	result := encodeBooleanData(boolValues)

	// Verify size matches what we expected
	if len(result) != int(originalSize) {
		return nil, fmt.Errorf("decompression size mismatch: expected %d, got %d", originalSize, len(result))
	}

	return result, nil
}

// decodeBooleanData decodes a byte slice into a slice of booleans
func decodeBooleanData(data []byte) ([]bool, error) {
	if len(data) == 0 {
		return []bool{}, nil
	}

	buf := bytes.NewReader(data)

	// Read the number of booleans
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	booleans := make([]bool, count)

	// Read each boolean value
	for i := uint32(0); i < count; i++ {
		var b byte
		if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
			return nil, err
		}
		booleans[i] = b != 0
	}

	return booleans, nil
}

// encodeBooleanData encodes a slice of booleans into a byte slice
func encodeBooleanData(booleans []bool) []byte {
	var buf bytes.Buffer

	// Write the number of booleans
	count := uint32(len(booleans))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each boolean value
	for _, b := range booleans {
		var byteVal byte
		if b {
			byteVal = 1
		} else {
			byteVal = 0
		}
		binary.Write(&buf, binary.LittleEndian, byteVal)
	}

	return buf.Bytes()
}
