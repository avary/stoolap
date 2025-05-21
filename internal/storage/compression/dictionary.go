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
)

var (
	ErrInvalidData          = errors.New("invalid data for dictionary encoding")
	ErrValueNotInDictionary = errors.New("value not found in dictionary")
)

// DictionaryEncoder implements dictionary encoding for string values
type DictionaryEncoder struct {
	// dictionary maps string values to their integer ids
	dictionary map[string]uint32
	// reverseDictionary maps ids back to string values (for decompression)
	reverseDictionary []string
	// nextID is the next available ID for a new dictionary entry
	nextID uint32
	// compressed tracks whether the data is in compressed form
	compressed bool
}

// NewDictionaryEncoder creates a new dictionary encoder
func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{
		dictionary:        make(map[string]uint32),
		reverseDictionary: make([]string, 0),
		nextID:            0,
		compressed:        false,
	}
}

// AddValue adds a string value to the dictionary if it doesn't exist
// and returns its ID
func (d *DictionaryEncoder) AddValue(value string) uint32 {
	if d.compressed {
		panic("cannot add values when dictionary is compressed")
	}

	id, exists := d.dictionary[value]
	if !exists {
		id = d.nextID
		d.dictionary[value] = id
		d.reverseDictionary = append(d.reverseDictionary, value)
		d.nextID++
	}
	return id
}

// GetID returns the ID for a given string value
func (d *DictionaryEncoder) GetID(value string) (uint32, bool) {
	id, exists := d.dictionary[value]
	return id, exists
}

// GetValue returns the string value for a given ID
func (d *DictionaryEncoder) GetValue(id uint32) (string, error) {
	if int(id) >= len(d.reverseDictionary) {
		return "", ErrValueNotInDictionary
	}
	return d.reverseDictionary[id], nil
}

// Cardinality returns the number of unique values in the dictionary
func (d *DictionaryEncoder) Cardinality() int {
	return len(d.dictionary)
}

// CompressStringBlock compresses a slice of strings using dictionary encoding
func (d *DictionaryEncoder) CompressStringBlock(values []string) ([]byte, error) {
	if d.compressed {
		return nil, errors.New("dictionary is already compressed")
	}

	// First pass: add all values to the dictionary
	for _, value := range values {
		d.AddValue(value)
	}

	// Optimization: Pre-allocate buffer with estimated size
	// Each value will be an uint32 (4 bytes) + header (4 bytes)
	bufSize := 4 + (len(values) * 4)
	buf := bytes.NewBuffer(make([]byte, 0, bufSize))

	// Write the number of values
	valCount := uint32(len(values))
	binary.Write(buf, binary.LittleEndian, valCount)

	// Optimization: Calculate bit width needed to represent the dictionary size
	// If dictionary has <= 256 entries, we need only 1 byte per ID
	// If dictionary has <= 65536 entries, we need only 2 bytes per ID
	useByte := d.nextID <= 256
	useShort := !useByte && d.nextID <= 65536

	// Write a flag indicating the encoding size (1=byte, 2=short, 4=int)
	var sizeFlag uint8
	if useByte {
		sizeFlag = 1
	} else if useShort {
		sizeFlag = 2
	} else {
		sizeFlag = 4
	}
	binary.Write(buf, binary.LittleEndian, sizeFlag)

	// Write each value's ID with the appropriate size
	for _, value := range values {
		id := d.dictionary[value]
		if useByte {
			binary.Write(buf, binary.LittleEndian, uint8(id))
		} else if useShort {
			binary.Write(buf, binary.LittleEndian, uint16(id))
		} else {
			binary.Write(buf, binary.LittleEndian, id)
		}
	}

	d.compressed = true
	return buf.Bytes(), nil
}

// DecompressStringBlock decompresses data that was compressed with dictionary encoding
func (d *DictionaryEncoder) DecompressStringBlock(data []byte) ([]string, error) {
	buf := bytes.NewReader(data)

	// Read the number of values
	var valCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &valCount); err != nil {
		return nil, err
	}

	// Read the encoding size flag
	var sizeFlag uint8
	if err := binary.Read(buf, binary.LittleEndian, &sizeFlag); err != nil {
		return nil, err
	}

	// Pre-allocate result slice for better performance
	result := make([]string, valCount)

	// Read IDs based on the encoding size
	for i := uint32(0); i < valCount; i++ {
		var id uint32

		// Read the ID with the appropriate size
		switch sizeFlag {
		case 1: // byte
			var byteID uint8
			if err := binary.Read(buf, binary.LittleEndian, &byteID); err != nil {
				return nil, err
			}
			id = uint32(byteID)
		case 2: // short
			var shortID uint16
			if err := binary.Read(buf, binary.LittleEndian, &shortID); err != nil {
				return nil, err
			}
			id = uint32(shortID)
		case 4: // int
			if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("invalid size flag: %d", sizeFlag)
		}

		// Validate ID and look up string
		if int(id) >= len(d.reverseDictionary) {
			return nil, fmt.Errorf("invalid dictionary ID: %d", id)
		}
		result[i] = d.reverseDictionary[id]
	}

	return result, nil
}

// Reset resets the dictionary encoder to its initial state
func (d *DictionaryEncoder) Reset() {
	d.dictionary = make(map[string]uint32)
	d.reverseDictionary = make([]string, 0)
	d.nextID = 0
	d.compressed = false
}

// SerializeDictionary serializes the dictionary to bytes
func (d *DictionaryEncoder) SerializeDictionary() ([]byte, error) {
	// Estimate dictionary size for buffer pre-allocation
	// Each entry has 4 bytes for string length + string data + some overhead
	estimatedSize := 4 // dictionary size header
	for _, str := range d.reverseDictionary {
		estimatedSize += 4 + len(str) // string length + data
	}

	// Pre-allocate buffer
	buf := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// Write the number of entries in the dictionary
	dictSize := uint32(len(d.dictionary))
	if err := binary.Write(buf, binary.LittleEndian, dictSize); err != nil {
		return nil, err
	}

	// Calculate if we need to store string offsets (optimization for faster lookup)
	storeOffsets := dictSize > 100 // Only worth the overhead for large dictionaries

	// If storing offsets, reserve space for the offset table
	var offsetStart int
	if storeOffsets {
		// Write a flag indicating we're using offsets
		if err := buf.WriteByte(1); err != nil {
			return nil, err
		}

		// Remember position to write offsets
		offsetStart = buf.Len()

		// Reserve space for offsets (each is a uint32)
		offsetsSize := int(dictSize) * 4
		nullBytes := make([]byte, offsetsSize)
		if _, err := buf.Write(nullBytes); err != nil {
			return nil, err
		}
	} else {
		// Write a flag indicating we're not using offsets
		if err := buf.WriteByte(0); err != nil {
			return nil, err
		}
	}

	// Store string data and build offsets if needed
	offsets := make([]uint32, dictSize)
	stringDataStart := buf.Len()

	for i := uint32(0); i < dictSize; i++ {
		if storeOffsets {
			// Remember offset to this string
			offsets[i] = uint32(buf.Len() - stringDataStart)
		}

		value := d.reverseDictionary[i]

		// Write string length
		strLen := uint32(len(value))
		if err := binary.Write(buf, binary.LittleEndian, strLen); err != nil {
			return nil, err
		}

		// Write string data
		if _, err := buf.Write([]byte(value)); err != nil {
			return nil, err
		}
	}

	// If we're using offsets, go back and write them
	if storeOffsets {
		// Get the current bytes
		data := buf.Bytes()

		// Write each offset to its proper position
		for i, offset := range offsets {
			pos := offsetStart + (i * 4)
			binary.LittleEndian.PutUint32(data[pos:pos+4], offset)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeDictionary deserializes the dictionary from bytes
func (d *DictionaryEncoder) DeserializeDictionary(data []byte) error {
	buf := bytes.NewReader(data)

	// Read the number of entries in the dictionary
	var dictSize uint32
	if err := binary.Read(buf, binary.LittleEndian, &dictSize); err != nil {
		return err
	}

	// Read offset flag
	var offsetFlag byte
	if err := binary.Read(buf, binary.LittleEndian, &offsetFlag); err != nil {
		return err
	}

	// Reset the dictionary
	d.Reset()

	// Pre-allocate for better performance
	d.dictionary = make(map[string]uint32, dictSize)
	d.reverseDictionary = make([]string, 0, dictSize)

	// Read offsets if used
	var offsets []uint32
	if offsetFlag == 1 {
		// Read offset table
		offsets = make([]uint32, dictSize)
		for i := uint32(0); i < dictSize; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &offsets[i]); err != nil {
				return err
			}
		}
	}

	// If using offsets, remember where string data starts
	stringDataStart := int64(0)
	if offsetFlag == 1 {
		// bytes.Reader doesn't expose its size, so we'll just track position
		stringDataStart = int64(len(data) - buf.Len())
	}

	// Read each dictionary entry
	for i := uint32(0); i < dictSize; i++ {
		// If using offsets, seek to correct position
		if offsetFlag == 1 && i > 0 {
			offset := offsets[i]
			if _, err := buf.Seek(stringDataStart+int64(offset), 0); err != nil {
				return err
			}
		}

		// Read string length
		var strLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return err
		}

		// Read string data
		strData := make([]byte, strLen)
		if _, err := buf.Read(strData); err != nil {
			return err
		}

		value := string(strData)
		d.dictionary[value] = i
		d.reverseDictionary = append(d.reverseDictionary, value)
		d.nextID++
	}

	return nil
}
