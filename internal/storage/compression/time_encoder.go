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
	"time"
)

// TimeEncodingFormat specifies different encoding formats for time values
type TimeEncodingFormat uint8

const (
	// FullTime stores the complete timestamp
	FullTime TimeEncodingFormat = iota

	// DateOnly stores only date components (year, month, day)
	DateOnly

	// TimeOnly stores only time components (hour, minute, second, nanosecond)
	TimeOnly

	// DeltaEncoding stores base time and delta values
	DeltaEncoding
)

// TimeEncoder implements specialized compression for time.Time values
type TimeEncoder struct {
	format TimeEncodingFormat
}

// NewTimeEncoder creates a new time encoder with the specified format
func NewTimeEncoder(format TimeEncodingFormat) *TimeEncoder {
	return &TimeEncoder{format: format}
}

// Encode compresses a slice of time.Time values
func (e *TimeEncoder) Encode(timestamps []time.Time) ([]byte, error) {
	if len(timestamps) == 0 {
		return []byte{}, nil
	}

	// Create a buffer for the result
	buf := bytes.Buffer{}

	// Write the encoding format
	buf.WriteByte(byte(e.format))

	// Write the number of timestamps
	binary.Write(&buf, binary.LittleEndian, uint32(len(timestamps)))

	switch e.format {
	case FullTime:
		return e.encodeFullTime(timestamps, &buf)
	case DeltaEncoding:
		return e.encodeDelta(timestamps, &buf)
	default:
		return nil, errors.New("unsupported time encoding format")
	}
}

// Decode decompresses a byte slice into a slice of time.Time values
func (e *TimeEncoder) Decode(data []byte) ([]time.Time, error) {
	if len(data) == 0 {
		return []time.Time{}, nil
	}

	// Create a reader for the data
	reader := bytes.NewReader(data)

	// Read the encoding format
	formatByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	format := TimeEncodingFormat(formatByte)

	// Read the number of timestamps
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	// Create a slice for the result
	timestamps := make([]time.Time, count)

	switch format {
	case FullTime:
		return e.decodeFullTime(reader, timestamps)
	case DeltaEncoding:
		return e.decodeDelta(reader, timestamps)
	default:
		return nil, errors.New("unsupported time encoding format")
	}
}

// encodeFullTime encodes timestamps with full precision
func (e *TimeEncoder) encodeFullTime(timestamps []time.Time, buf *bytes.Buffer) ([]byte, error) {
	// For full time, we simply write each timestamp as a Unix timestamp (seconds since epoch)
	// and nanoseconds (for sub-second precision)
	for _, ts := range timestamps {
		// Unix seconds since epoch (int64)
		binary.Write(buf, binary.LittleEndian, ts.Unix())

		// Nanoseconds portion (int32)
		binary.Write(buf, binary.LittleEndian, int32(ts.Nanosecond()))
	}

	return buf.Bytes(), nil
}

// encodeDelta encodes timestamps as delta values from a base timestamp
func (e *TimeEncoder) encodeDelta(timestamps []time.Time, buf *bytes.Buffer) ([]byte, error) {
	if len(timestamps) == 0 {
		return buf.Bytes(), nil
	}

	// Use the first timestamp as the base
	baseTime := timestamps[0]

	// Write the base timestamp
	binary.Write(buf, binary.LittleEndian, baseTime.Unix())
	binary.Write(buf, binary.LittleEndian, int32(baseTime.Nanosecond()))

	// Determine the smallest possible storage size for deltas
	// by analyzing the time range
	maxDelta := int64(0)
	for i := 1; i < len(timestamps); i++ {
		delta := timestamps[i].Sub(baseTime).Nanoseconds()
		if delta < 0 {
			delta = -delta
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}

	// Choose the smallest possible encoding size
	var deltaSize byte
	if maxDelta < (1 << 8) {
		deltaSize = 1 // Use 1 byte
	} else if maxDelta < (1 << 16) {
		deltaSize = 2 // Use 2 bytes
	} else if maxDelta < (1 << 32) {
		deltaSize = 4 // Use 4 bytes
	} else {
		deltaSize = 8 // Use 8 bytes
	}

	// Write the delta size
	buf.WriteByte(deltaSize)

	// Write each delta
	for i := 1; i < len(timestamps); i++ {
		delta := timestamps[i].Sub(baseTime).Nanoseconds()

		// Write the delta with the chosen size
		switch deltaSize {
		case 1:
			buf.WriteByte(byte(delta))
		case 2:
			binary.Write(buf, binary.LittleEndian, int16(delta))
		case 4:
			binary.Write(buf, binary.LittleEndian, int32(delta))
		case 8:
			binary.Write(buf, binary.LittleEndian, delta)
		}
	}

	return buf.Bytes(), nil
}

// decodeFullTime decodes full precision timestamps
func (e *TimeEncoder) decodeFullTime(reader *bytes.Reader, timestamps []time.Time) ([]time.Time, error) {
	for i := 0; i < len(timestamps); i++ {
		// Read Unix seconds
		var seconds int64
		if err := binary.Read(reader, binary.LittleEndian, &seconds); err != nil {
			return nil, err
		}

		// Read nanoseconds
		var nanos int32
		if err := binary.Read(reader, binary.LittleEndian, &nanos); err != nil {
			return nil, err
		}

		// Create the timestamp
		timestamps[i] = time.Unix(seconds, int64(nanos))
	}

	return timestamps, nil
}

// decodeDelta decodes delta-encoded timestamps
func (e *TimeEncoder) decodeDelta(reader *bytes.Reader, timestamps []time.Time) ([]time.Time, error) {
	// Read the base timestamp
	var baseSeconds int64
	if err := binary.Read(reader, binary.LittleEndian, &baseSeconds); err != nil {
		return nil, err
	}

	var baseNanos int32
	if err := binary.Read(reader, binary.LittleEndian, &baseNanos); err != nil {
		return nil, err
	}

	baseTime := time.Unix(baseSeconds, int64(baseNanos))

	// The first timestamp is the base time
	if len(timestamps) > 0 {
		timestamps[0] = baseTime
	}

	// Read the delta size
	deltaSizeByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	deltaSize := deltaSizeByte

	// Read each delta
	for i := 1; i < len(timestamps); i++ {
		var deltaNanos int64

		// Read the delta with the appropriate size
		switch deltaSize {
		case 1:
			b, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			deltaNanos = int64(b)
		case 2:
			var val int16
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			deltaNanos = int64(val)
		case 4:
			var val int32
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			deltaNanos = int64(val)
		case 8:
			if err := binary.Read(reader, binary.LittleEndian, &deltaNanos); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("invalid delta size")
		}

		// Apply the delta to the base time
		timestamps[i] = baseTime.Add(time.Duration(deltaNanos))
	}

	return timestamps, nil
}
