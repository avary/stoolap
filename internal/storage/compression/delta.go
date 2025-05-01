package compression

import (
	"bytes"
	"encoding/binary"
	"math"
)

// DeltaEncoder is a compression method that encodes the differences between consecutive values
// rather than the absolute values. This is particularly effective for sorted or incrementally
// changing numerical data.
type DeltaEncoder struct {
	// isFloat indicates whether we're encoding floats or integers
	isFloat bool
}

// NewDeltaEncoder creates a new delta encoder
func NewDeltaEncoder(isFloat bool) *DeltaEncoder {
	return &DeltaEncoder{
		isFloat: isFloat,
	}
}

// Encode takes a slice of bytes representing numerical values (ints or floats)
// and encodes them using delta encoding
func (e *DeltaEncoder) Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// For delta encoding, we need to determine the data type and size
	itemSize := 8 // Default to 8 bytes (int64/float64)
	if len(data)%4 == 0 && len(data)%8 != 0 {
		itemSize = 4 // 4 bytes (int32/float32)
	}

	// Calculate how many items we have
	itemCount := len(data) / itemSize

	if itemCount == 0 {
		return []byte{}, nil
	}

	// The result will have:
	// - 1 byte for isFloat flag
	// - 1 byte for itemSize
	// - itemSize bytes for base value
	// - Variable number of bytes for delta values
	// We'll pre-allocate a buffer that's large enough for most cases
	// but can grow if needed
	result := bytes.NewBuffer(make([]byte, 0, len(data)))

	// Write the header
	result.WriteByte(boolToByte(e.isFloat))
	result.WriteByte(byte(itemSize))

	// Keep track of previous value
	var prevIntValue int64
	var prevFloatValue float64

	// Process first value specially - store as absolute value
	if e.isFloat {
		if itemSize == 4 {
			var val float32
			binary.Read(bytes.NewReader(data[:4]), binary.LittleEndian, &val)
			prevFloatValue = float64(val)
			binary.Write(result, binary.LittleEndian, val)
		} else {
			binary.Read(bytes.NewReader(data[:8]), binary.LittleEndian, &prevFloatValue)
			binary.Write(result, binary.LittleEndian, prevFloatValue)
		}
	} else {
		if itemSize == 4 {
			var val int32
			binary.Read(bytes.NewReader(data[:4]), binary.LittleEndian, &val)
			prevIntValue = int64(val)
			binary.Write(result, binary.LittleEndian, val)
		} else {
			binary.Read(bytes.NewReader(data[:8]), binary.LittleEndian, &prevIntValue)
			binary.Write(result, binary.LittleEndian, prevIntValue)
		}
	}

	// Now encode deltas with variable encoding
	for i := 1; i < itemCount; i++ {
		start := i * itemSize
		end := start + itemSize

		if e.isFloat {
			var currValue float64
			if itemSize == 4 {
				var val float32
				binary.Read(bytes.NewReader(data[start:end]), binary.LittleEndian, &val)
				currValue = float64(val)
			} else {
				binary.Read(bytes.NewReader(data[start:end]), binary.LittleEndian, &currValue)
			}

			// For floats, we encode the difference with special handling for small values
			delta := currValue - prevFloatValue
			encodeFloatDelta(result, delta)
			prevFloatValue = currValue
		} else {
			var currValue int64
			if itemSize == 4 {
				var val int32
				binary.Read(bytes.NewReader(data[start:end]), binary.LittleEndian, &val)
				currValue = int64(val)
			} else {
				binary.Read(bytes.NewReader(data[start:end]), binary.LittleEndian, &currValue)
			}

			// For integers, calculate delta
			delta := currValue - prevIntValue
			encodeIntDelta(result, delta)
			prevIntValue = currValue
		}
	}

	return result.Bytes(), nil
}

// Decode takes delta-encoded data and reconstructs the original values
func (e *DeltaEncoder) Decode(encoded []byte) ([]byte, error) {
	if len(encoded) <= 2 {
		return []byte{}, nil
	}

	reader := bytes.NewReader(encoded)

	// Read header
	isFloatByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	isFloat := byteToBoolean(isFloatByte)

	itemSizeByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	itemSize := int(itemSizeByte)

	// Prepare buffer for results
	result := bytes.NewBuffer(make([]byte, 0, len(encoded)*2))

	// Read base value
	var baseIntValue int64
	var baseFloatValue float64

	if isFloat {
		if itemSize == 4 {
			var val float32
			err = binary.Read(reader, binary.LittleEndian, &val)
			if err != nil {
				return nil, err
			}
			baseFloatValue = float64(val)
			binary.Write(result, binary.LittleEndian, val)
		} else {
			err = binary.Read(reader, binary.LittleEndian, &baseFloatValue)
			if err != nil {
				return nil, err
			}
			binary.Write(result, binary.LittleEndian, baseFloatValue)
		}
	} else {
		if itemSize == 4 {
			var val int32
			err = binary.Read(reader, binary.LittleEndian, &val)
			if err != nil {
				return nil, err
			}
			baseIntValue = int64(val)
			binary.Write(result, binary.LittleEndian, val)
		} else {
			err = binary.Read(reader, binary.LittleEndian, &baseIntValue)
			if err != nil {
				return nil, err
			}
			binary.Write(result, binary.LittleEndian, baseIntValue)
		}
	}

	// Current values start as base values
	currIntValue := baseIntValue
	currFloatValue := baseFloatValue

	// Read deltas and reconstruct values until we reach the end of data
	for reader.Len() > 0 {
		if isFloat {
			delta, err := decodeFloatDelta(reader)
			if err != nil {
				return nil, err
			}

			currFloatValue += delta

			if itemSize == 4 {
				val := float32(currFloatValue)
				binary.Write(result, binary.LittleEndian, val)
			} else {
				binary.Write(result, binary.LittleEndian, currFloatValue)
			}
		} else {
			delta, err := decodeIntDelta(reader)
			if err != nil {
				return nil, err
			}

			currIntValue += delta

			if itemSize == 4 {
				val := int32(currIntValue)
				binary.Write(result, binary.LittleEndian, val)
			} else {
				binary.Write(result, binary.LittleEndian, currIntValue)
			}
		}
	}

	return result.Bytes(), nil
}

// Helper functions for variable-length encoding of deltas

// encodeIntDelta encodes an integer delta with variable-length encoding
// Small deltas use fewer bytes
func encodeIntDelta(buf *bytes.Buffer, delta int64) {
	// Check if delta can fit in 1 byte (-64 to 63)
	if delta >= -64 && delta < 64 {
		// Use 1 byte, with highest bit = 0
		buf.WriteByte(byte((delta & 0x7F) | 0x00))
		return
	}

	// Check if delta can fit in 2 bytes (-8192 to 8191)
	if delta >= -8192 && delta < 8192 {
		// Use 2 bytes, with highest bit of first byte = 1, second highest = 0
		buf.WriteByte(byte(((delta >> 8) & 0x3F) | 0x80))
		buf.WriteByte(byte(delta & 0xFF))
		return
	}

	// Check if delta can fit in 3 bytes (-1048576 to 1048575)
	if delta >= -1048576 && delta < 1048576 {
		// Use 3 bytes, with two highest bits of first byte = 10
		buf.WriteByte(byte(((delta >> 16) & 0x1F) | 0xC0))
		buf.WriteByte(byte((delta >> 8) & 0xFF))
		buf.WriteByte(byte(delta & 0xFF))
		return
	}

	// Check if delta can fit in 4 bytes (-134217728 to 134217727)
	if delta >= -134217728 && delta < 134217728 {
		// Use 4 bytes, with two highest bits of first byte = 11, third highest = 0
		buf.WriteByte(byte(((delta >> 24) & 0x0F) | 0xE0))
		buf.WriteByte(byte((delta >> 16) & 0xFF))
		buf.WriteByte(byte((delta >> 8) & 0xFF))
		buf.WriteByte(byte(delta & 0xFF))
		return
	}

	// Use full 8 bytes, with 3 highest bits of first byte = 111
	buf.WriteByte(0xF0)
	binary.Write(buf, binary.LittleEndian, delta)
}

// decodeIntDelta decodes an integer delta with variable-length encoding
func decodeIntDelta(reader *bytes.Reader) (int64, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Check first byte pattern to determine encoding length
	if b&0x80 == 0 {
		// 1-byte encoding (-64 to 63)
		return int64(int8(b<<1) >> 1), nil
	}

	if b&0xC0 == 0x80 {
		// 2-byte encoding (-8192 to 8191)
		b2, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		// Reconstruct value from 2 bytes
		return int64(int16(((int16(b&0x3F)<<8)|int16(b2))<<2) >> 2), nil
	}

	if b&0xE0 == 0xC0 {
		// 3-byte encoding (-1048576 to 1048575)
		b2, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		b3, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		// Reconstruct value from 3 bytes
		return int64(int32(((int32(b&0x1F)<<16)|(int32(b2)<<8)|int32(b3))<<3) >> 3), nil
	}

	if b&0xF0 == 0xE0 {
		// 4-byte encoding (-134217728 to 134217727)
		b2, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		b3, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		b4, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		// Reconstruct value from 4 bytes
		return int64(int32(((int32(b&0x0F)<<24)|(int32(b2)<<16)|(int32(b3)<<8)|int32(b4))<<4) >> 4), nil
	}

	if b == 0xF0 {
		// 8-byte encoding (full int64)
		var delta int64
		err = binary.Read(reader, binary.LittleEndian, &delta)
		if err != nil {
			return 0, err
		}
		return delta, nil
	}

	// This should never happen with proper encoding
	return 0, nil
}

// encodeFloatDelta encodes a floating-point delta with special handling
func encodeFloatDelta(buf *bytes.Buffer, delta float64) {
	// Special case for zero delta (common in time series)
	if delta == 0 {
		buf.WriteByte(0)
		return
	}

	// If small integer delta, encode efficiently
	if math.Floor(delta) == delta && delta >= -127 && delta <= 127 {
		buf.WriteByte(1)
		buf.WriteByte(byte(int8(delta)))
		return
	}

	// For float32-representable values
	float32Val := float32(delta)
	if float64(float32Val) == delta {
		buf.WriteByte(2)
		binary.Write(buf, binary.LittleEndian, float32Val)
		return
	}

	// Fall back to full float64
	buf.WriteByte(3)
	binary.Write(buf, binary.LittleEndian, delta)
}

// decodeFloatDelta decodes a floating-point delta
func decodeFloatDelta(reader *bytes.Reader) (float64, error) {
	code, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch code {
	case 0:
		// Zero delta
		return 0, nil

	case 1:
		// Small integer delta
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return float64(int8(b)), nil

	case 2:
		// Float32 delta
		var val float32
		err = binary.Read(reader, binary.LittleEndian, &val)
		if err != nil {
			return 0, err
		}
		return float64(val), nil

	case 3:
		// Full float64 delta
		var val float64
		err = binary.Read(reader, binary.LittleEndian, &val)
		if err != nil {
			return 0, err
		}
		return val, nil
	}

	// Should never happen with proper encoding
	return 0, nil
}

// Helper functions for boolean conversion
func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func byteToBoolean(b byte) bool {
	return b != 0
}
