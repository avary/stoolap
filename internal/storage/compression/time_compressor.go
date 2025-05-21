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
	"errors"
	"fmt"
	"time"
)

// TimeCompressor implements the Compressor interface for time.Time values
type TimeCompressor struct {
	encoder *TimeEncoder
}

// NewTimeCompressor creates a new time compressor with the specified format
func NewTimeCompressor(format TimeEncodingFormat) *TimeCompressor {
	return &TimeCompressor{
		encoder: NewTimeEncoder(format),
	}
}

// Type returns the compression type (a new constant should be added in compression.go)
func (c *TimeCompressor) Type() CompressionType {
	return TimeCompression
}

// Compress compresses time values
func (c *TimeCompressor) Compress(data []byte) ([]byte, error) {
	// Decode the time values from the input byte slice
	timeValues, err := decodeTimeData(data)
	if err != nil {
		return nil, err
	}

	// Compress using the time encoder
	compressedData, err := c.encoder.Encode(timeValues)
	if err != nil {
		return nil, err
	}

	// Create the result buffer with header
	result := make([]byte, CompressionHeaderSize+len(compressedData))

	// Set compression type
	result[0] = byte(c.Type())

	// Set original data size
	binary.LittleEndian.PutUint32(result[1:CompressionHeaderSize], uint32(len(data)))

	// Copy compressed data
	copy(result[CompressionHeaderSize:], compressedData)

	return result, nil
}

// Decompress decompresses time-compressed data
func (c *TimeCompressor) Decompress(data []byte) ([]byte, error) {
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

	// Decode the compressed data to time values
	timeValues, err := c.encoder.Decode(compressedData)
	if err != nil {
		return nil, err
	}

	// Encode time values back to the original format
	result := encodeTimeData(timeValues)

	// Verify size matches what we expected
	if len(result) != int(originalSize) {
		return nil, fmt.Errorf("decompression size mismatch: expected %d, got %d", originalSize, len(result))
	}

	return result, nil
}

// decodeTimeData decodes a byte slice into a slice of time.Time values
func decodeTimeData(data []byte) ([]time.Time, error) {
	if len(data) == 0 {
		return []time.Time{}, nil
	}

	buf := bytes.NewReader(data)

	// Read the number of time values
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	timeValues := make([]time.Time, count)

	// Read each time value
	for i := uint32(0); i < count; i++ {
		// Read Unix seconds
		var seconds int64
		if err := binary.Read(buf, binary.LittleEndian, &seconds); err != nil {
			return nil, err
		}

		// Read nanoseconds
		var nanos int32
		if err := binary.Read(buf, binary.LittleEndian, &nanos); err != nil {
			return nil, err
		}

		// Create the timestamp
		timeValues[i] = time.Unix(seconds, int64(nanos))
	}

	return timeValues, nil
}

// encodeTimeData encodes a slice of time.Time values into a byte slice
func encodeTimeData(timeValues []time.Time) []byte {
	var buf bytes.Buffer

	// Write the number of time values
	count := uint32(len(timeValues))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each time value
	for _, t := range timeValues {
		// Write Unix seconds
		binary.Write(&buf, binary.LittleEndian, t.Unix())

		// Write nanoseconds
		binary.Write(&buf, binary.LittleEndian, int32(t.Nanosecond()))
	}

	return buf.Bytes()
}
