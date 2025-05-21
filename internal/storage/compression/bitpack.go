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
	"encoding/binary"
	"errors"
)

// BitPackEncoder implements bit packing for boolean values
// This allows storing 8 boolean values in a single byte
type BitPackEncoder struct{}

// NewBitPackEncoder creates a new bit pack encoder
func NewBitPackEncoder() *BitPackEncoder {
	return &BitPackEncoder{}
}

// Encode takes a slice of boolean values and packs them into bytes
// where each boolean uses only 1 bit instead of a full byte
func (e *BitPackEncoder) Encode(data []bool) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Calculate the number of bytes needed to store all bits
	// We need (len(data) + 7) / 8 bytes to store len(data) bits
	bytesNeeded := (len(data) + 7) / 8

	// Allocate result buffer with additional space for the length
	resultBuffer := make([]byte, 4+bytesNeeded)

	// Store the original length at the beginning (4 bytes)
	binary.LittleEndian.PutUint32(resultBuffer[0:4], uint32(len(data)))

	// Pack bits into bytes
	for i := 0; i < len(data); i++ {
		if data[i] {
			// Calculate which byte and bit position this boolean value maps to
			bytePos := 4 + (i / 8)
			bitPos := i % 8

			// Set the corresponding bit
			// First, get the current byte value
			byteVal := resultBuffer[bytePos]

			// Set the bit at position bitPos
			// (using 1 << bitPos to create a byte with only the bit at position bitPos set)
			resultBuffer[bytePos] = byteVal | (1 << bitPos)
		}
	}

	return resultBuffer, nil
}

// Decode takes bit-packed data and unpacks it into boolean values
func (e *BitPackEncoder) Decode(data []byte) ([]bool, error) {
	if len(data) < 4 {
		return []bool{}, errors.New("invalid data: too short for BitPack encoding")
	}

	// Read the original length
	originalLength := binary.LittleEndian.Uint32(data[0:4])

	// Create result slice with the original length
	result := make([]bool, originalLength)

	// Extract each bit
	for i := uint32(0); i < originalLength; i++ {
		bytePos := 4 + (i / 8)
		bitPos := i % 8

		// If we've run out of bytes, fill remaining with false
		if int(bytePos) >= len(data) {
			break
		}

		// Check if the bit at position bitPos is set
		result[i] = (data[bytePos] & (1 << bitPos)) != 0
	}

	return result, nil
}
