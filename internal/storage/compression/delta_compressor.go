package compression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// DeltaCompressor implements the Compressor interface for delta encoding
type DeltaCompressor struct {
	// encoder is the DeltaEncoder used for compression/decompression
	encoder *DeltaEncoder
}

// NewDeltaCompressor creates a new delta compressor for the specified numeric type
func NewDeltaCompressor(isFloat bool) *DeltaCompressor {
	return &DeltaCompressor{
		encoder: NewDeltaEncoder(isFloat),
	}
}

// Compress compresses the given data using delta encoding
// The expected data format is a sequence of numeric values (int32, int64, float32, float64)
func (c *DeltaCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Compress using delta encoding
	encoded, err := c.encoder.Encode(data)
	if err != nil {
		return nil, err
	}

	// Create the result buffer with header
	result := make([]byte, CompressionHeaderSize+len(encoded))

	// Set compression type
	result[0] = byte(c.Type())

	// Set original data size
	binary.LittleEndian.PutUint32(result[1:CompressionHeaderSize], uint32(len(data)))

	// Copy encoded data
	copy(result[CompressionHeaderSize:], encoded)

	return result, nil
}

// Decompress decompresses delta-encoded data
func (c *DeltaCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

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

	// Decompress the data
	decompressed, err := c.encoder.Decode(compressedData)
	if err != nil {
		return nil, err
	}

	// Verify size matches what we expected
	if len(decompressed) != int(originalSize) {
		return nil, fmt.Errorf("decompression size mismatch: expected %d, got %d", originalSize, len(decompressed))
	}

	return decompressed, nil
}

// Type returns the compression type (Delta)
func (c *DeltaCompressor) Type() CompressionType {
	return Delta
}

// EstimateCompressedSize attempts to predict the compressed size for a given data slice
// This is used to decide whether to apply compression or not
func EstimateDeltaCompressedSize(data []byte, isFloat bool) int {
	if len(data) == 0 {
		return 0
	}

	// For very small inputs, compression might not be beneficial
	if len(data) < 32 {
		return len(data)
	}

	// Determine item size
	itemSize := 8 // Default to 8 bytes (int64/float64)
	if len(data)%4 == 0 && len(data)%8 != 0 {
		itemSize = 4 // 4 bytes (int32/float32)
	}

	// Calculate number of items
	itemCount := len(data) / itemSize
	if itemCount <= 1 {
		return len(data)
	}

	// Sample a portion of the data to estimate delta size
	sampleSize := itemCount
	if sampleSize > 100 {
		sampleSize = 100
	}

	// Start with header size
	estimatedSize := CompressionHeaderSize

	// Add size for storing the first value (uncompressed)
	estimatedSize += itemSize

	// For integer data, estimate using heuristics based on value ranges
	if !isFloat {
		// Sample some deltas to get a better estimate
		prevValue := int64(0)
		smallDeltaCount := 0
		mediumDeltaCount := 0
		largeDeltaCount := 0

		// Read first value
		reader := bytes.NewReader(data)
		if itemSize == 4 {
			var val int32
			binary.Read(reader, binary.LittleEndian, &val)
			prevValue = int64(val)
		} else {
			binary.Read(reader, binary.LittleEndian, &prevValue)
		}

		// Sample deltas
		sampleInterval := itemCount / sampleSize
		if sampleInterval < 1 {
			sampleInterval = 1
		}

		for i := 1; i < itemCount; i += sampleInterval {
			pos := i * itemSize
			if pos+itemSize > len(data) {
				break
			}

			currValue := int64(0)
			reader := bytes.NewReader(data[pos : pos+itemSize])
			if itemSize == 4 {
				var val int32
				binary.Read(reader, binary.LittleEndian, &val)
				currValue = int64(val)
			} else {
				binary.Read(reader, binary.LittleEndian, &currValue)
			}

			delta := currValue - prevValue
			prevValue = currValue

			// Classify delta
			if delta >= -64 && delta < 64 {
				smallDeltaCount++
			} else if delta >= -8192 && delta < 8192 {
				mediumDeltaCount++
			} else {
				largeDeltaCount++
			}
		}

		// Calculate estimated size based on delta distribution
		smallDeltaSize := 1
		mediumDeltaSize := 2
		largeMultiByteSize := 5 // Average between 3, 4, and 8-byte encodings

		totalDeltaCount := smallDeltaCount + mediumDeltaCount + largeDeltaCount
		if totalDeltaCount == 0 {
			// Fall back to optimistic estimate
			estimatedSize += (itemCount - 1) * 2
		} else {
			smallDeltaRatio := float64(smallDeltaCount) / float64(totalDeltaCount)
			mediumDeltaRatio := float64(mediumDeltaCount) / float64(totalDeltaCount)
			largeDeltaRatio := float64(largeDeltaCount) / float64(totalDeltaCount)

			avgDeltaSize := float64(smallDeltaSize)*smallDeltaRatio +
				float64(mediumDeltaSize)*mediumDeltaRatio +
				float64(largeMultiByteSize)*largeDeltaRatio

			// Apply average delta size to all deltas
			estimatedSize += int(float64(itemCount-1) * avgDeltaSize)
		}
	} else {
		// For float data, estimate differently
		// Floating-point deltas usually take more bytes due to their nature
		// but can still benefit from delta encoding, especially if they have patterns

		// Sample some deltas to get a better estimate
		prevValue := float64(0)
		zeroDeltaCount := 0
		smallIntDeltaCount := 0
		float32DeltaCount := 0
		float64DeltaCount := 0

		// Read first value
		reader := bytes.NewReader(data)
		if itemSize == 4 {
			var val float32
			binary.Read(reader, binary.LittleEndian, &val)
			prevValue = float64(val)
		} else {
			binary.Read(reader, binary.LittleEndian, &prevValue)
		}

		// Sample deltas
		sampleInterval := itemCount / sampleSize
		if sampleInterval < 1 {
			sampleInterval = 1
		}

		for i := 1; i < itemCount; i += sampleInterval {
			pos := i * itemSize
			if pos+itemSize > len(data) {
				break
			}

			currValue := float64(0)
			reader := bytes.NewReader(data[pos : pos+itemSize])
			if itemSize == 4 {
				var val float32
				binary.Read(reader, binary.LittleEndian, &val)
				currValue = float64(val)
			} else {
				binary.Read(reader, binary.LittleEndian, &currValue)
			}

			delta := currValue - prevValue
			prevValue = currValue

			// Classify delta
			if delta == 0 {
				zeroDeltaCount++
			} else if math.Floor(delta) == delta && delta >= -127 && delta <= 127 {
				smallIntDeltaCount++
			} else {
				// Check if float32 is sufficient
				float32Val := float32(delta)
				if float64(float32Val) == delta {
					float32DeltaCount++
				} else {
					float64DeltaCount++
				}
			}
		}

		// Calculate estimated size based on delta distribution
		zeroDeltaSize := 1
		smallIntDeltaSize := 2 // 1 byte for code + 1 byte for value
		float32DeltaSize := 5  // 1 byte for code + 4 bytes for float32
		float64DeltaSize := 9  // 1 byte for code + 8 bytes for float64

		totalDeltaCount := zeroDeltaCount + smallIntDeltaCount + float32DeltaCount + float64DeltaCount
		if totalDeltaCount == 0 {
			// Fall back to pessimistic estimate
			estimatedSize += (itemCount - 1) * float64DeltaSize
		} else {
			zeroDeltaRatio := float64(zeroDeltaCount) / float64(totalDeltaCount)
			smallIntDeltaRatio := float64(smallIntDeltaCount) / float64(totalDeltaCount)
			float32DeltaRatio := float64(float32DeltaCount) / float64(totalDeltaCount)
			float64DeltaRatio := float64(float64DeltaCount) / float64(totalDeltaCount)

			avgDeltaSize := float64(zeroDeltaSize)*zeroDeltaRatio +
				float64(smallIntDeltaSize)*smallIntDeltaRatio +
				float64(float32DeltaSize)*float32DeltaRatio +
				float64(float64DeltaSize)*float64DeltaRatio

			// Apply average delta size to all deltas
			estimatedSize += int(float64(itemCount-1) * avgDeltaSize)
		}
	}

	return estimatedSize
}
