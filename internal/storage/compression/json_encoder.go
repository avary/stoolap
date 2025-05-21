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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// JSONEncoder implements specialized compression for JSON data
type JSONEncoder struct{}

// NewJSONEncoder creates a new JSON encoder
func NewJSONEncoder() *JSONEncoder {
	return &JSONEncoder{}
}

// JSONEncodingFormat represents the type of JSON encoding format to use
type JSONEncodingFormat uint8

const (
	// StandardJSON uses standard JSON with no compression
	StandardJSON JSONEncodingFormat = iota

	// FieldNameDictionary replaces repeated field names with numeric IDs
	FieldNameDictionary

	// SchemaOptimized uses schema optimization for repeated structures
	SchemaOptimized
)

// fieldDictionary maps field names to their IDs for efficient storage
type fieldDictionary struct {
	fieldToID map[string]uint16
	idToField []string
	nextID    uint16
}

// newFieldDictionary creates a new field dictionary
func newFieldDictionary() *fieldDictionary {
	return &fieldDictionary{
		fieldToID: make(map[string]uint16),
		idToField: make([]string, 0),
		nextID:    0,
	}
}

// add adds a field name to the dictionary if it doesn't exist yet
func (d *fieldDictionary) add(field string) uint16 {
	id, exists := d.fieldToID[field]
	if !exists {
		id = d.nextID
		d.fieldToID[field] = id
		d.idToField = append(d.idToField, field)
		d.nextID++
	}
	return id
}

// get returns the field name for a given ID
func (d *fieldDictionary) get(id uint16) (string, error) {
	if int(id) >= len(d.idToField) {
		return "", fmt.Errorf("field ID %d not found in dictionary", id)
	}
	return d.idToField[id], nil
}

// serialize serializes the dictionary to bytes
func (d *fieldDictionary) serialize() []byte {
	var buf bytes.Buffer

	// Write the number of fields
	count := uint16(len(d.idToField))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each field name
	for _, field := range d.idToField {
		// Write field length
		fieldLen := uint16(len(field))
		binary.Write(&buf, binary.LittleEndian, fieldLen)

		// Write field data
		buf.Write([]byte(field))
	}

	return buf.Bytes()
}

// deserialize deserializes the dictionary from bytes
func (d *fieldDictionary) deserialize(data []byte) error {
	reader := bytes.NewReader(data)

	// Read the number of fields
	var count uint16
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return err
	}

	// Initialize the dictionary
	d.fieldToID = make(map[string]uint16, count)
	d.idToField = make([]string, count)

	// Read each field name
	for i := uint16(0); i < count; i++ {
		// Read field length
		var fieldLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &fieldLen); err != nil {
			return err
		}

		// Read field data
		fieldData := make([]byte, fieldLen)
		if _, err := reader.Read(fieldData); err != nil {
			return err
		}

		field := string(fieldData)
		d.fieldToID[field] = i
		d.idToField[i] = field
	}

	d.nextID = count
	return nil
}

// Encode compresses JSON data using field name dictionary
func (e *JSONEncoder) Encode(jsonData [][]byte) ([]byte, error) {
	if len(jsonData) == 0 {
		return []byte{}, nil
	}

	// Choose the best compression method based on the data
	compressionMethod := e.chooseCompressionMethod(jsonData)

	// Create a buffer for the result
	buf := bytes.Buffer{}

	// Write the compression method
	buf.WriteByte(byte(compressionMethod))

	// Write the count of JSON objects
	binary.Write(&buf, binary.LittleEndian, uint32(len(jsonData)))

	switch compressionMethod {
	case StandardJSON:
		return e.encodeStandard(jsonData, &buf)
	case FieldNameDictionary:
		return e.encodeWithFieldDictionary(jsonData, &buf)
	case SchemaOptimized:
		// Not implemented yet, fall back to field dictionary
		return e.encodeWithFieldDictionary(jsonData, &buf)
	default:
		return nil, errors.New("unknown JSON compression method")
	}
}

// chooseCompressionMethod analyzes the data to determine the best compression method
func (e *JSONEncoder) chooseCompressionMethod(jsonData [][]byte) JSONEncodingFormat {
	// For very small datasets, standard JSON might be more efficient
	if len(jsonData) < 5 {
		return StandardJSON
	}

	// Count field occurrences to determine if field dictionary is beneficial
	fieldCount := make(map[string]int)
	totalFields := 0

	// Sample up to 20 objects to check field repetition
	sampleSize := 20
	if len(jsonData) < sampleSize {
		sampleSize = len(jsonData)
	}

	for i := 0; i < sampleSize; i++ {
		// Extract all field names from this JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal(jsonData[i], &obj); err != nil {
			// If there's a parsing error, default to standard JSON
			continue
		}

		// Count field occurrences
		for field := range obj {
			fieldCount[field]++
			totalFields++
		}
	}

	// If there are repeated fields, use field dictionary compression
	if len(fieldCount) < totalFields/2 { // At least 2x repetition on average
		return FieldNameDictionary
	}

	return StandardJSON
}

// encodeStandard encodes JSON data without compression
func (e *JSONEncoder) encodeStandard(jsonData [][]byte, buf *bytes.Buffer) ([]byte, error) {
	// For standard encoding, we just concatenate the JSON objects with length prefixes
	for _, obj := range jsonData {
		// Write length of the JSON object
		objLen := uint32(len(obj))
		binary.Write(buf, binary.LittleEndian, objLen)

		// Write the JSON object
		buf.Write(obj)
	}

	return buf.Bytes(), nil
}

// encodeWithFieldDictionary encodes JSON data using a field name dictionary
func (e *JSONEncoder) encodeWithFieldDictionary(jsonData [][]byte, buf *bytes.Buffer) ([]byte, error) {
	// Create a field dictionary
	dict := newFieldDictionary()

	// First pass: build the dictionary
	for _, obj := range jsonData {
		var data map[string]interface{}
		if err := json.Unmarshal(obj, &data); err != nil {
			// Skip invalid JSON objects
			continue
		}

		// Add all field names to the dictionary
		for field := range data {
			dict.add(field)
		}
	}

	// Serialize and write the dictionary
	dictData := dict.serialize()
	dictLen := uint32(len(dictData))
	binary.Write(buf, binary.LittleEndian, dictLen)
	buf.Write(dictData)

	// Second pass: encode each JSON object with the dictionary
	for _, obj := range jsonData {
		var data map[string]interface{}
		if err := json.Unmarshal(obj, &data); err != nil {
			// For invalid JSON, write a zero length
			binary.Write(buf, binary.LittleEndian, uint32(0))
			continue
		}

		// Encode the object using field IDs
		encodedObj, err := e.encodeObject(data, dict)
		if err != nil {
			return nil, err
		}

		// Write length of the encoded object
		encodedLen := uint32(len(encodedObj))
		binary.Write(buf, binary.LittleEndian, encodedLen)

		// Write the encoded object
		buf.Write(encodedObj)
	}

	return buf.Bytes(), nil
}

// encodeObject encodes a single JSON object using the field dictionary
func (e *JSONEncoder) encodeObject(obj map[string]interface{}, dict *fieldDictionary) ([]byte, error) {
	var buf bytes.Buffer

	// Get a sorted list of fields for consistent encoding
	fields := make([]string, 0, len(obj))
	for field := range obj {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	// Write the field count
	fieldCount := uint16(len(fields))
	binary.Write(&buf, binary.LittleEndian, fieldCount)

	// Encode each field with its value
	for _, field := range fields {
		// Write field ID from dictionary
		fieldID := dict.add(field)
		binary.Write(&buf, binary.LittleEndian, fieldID)

		// Encode the value
		valueBytes, err := json.Marshal(obj[field])
		if err != nil {
			return nil, err
		}

		// Write value length
		valueLen := uint32(len(valueBytes))
		binary.Write(&buf, binary.LittleEndian, valueLen)

		// Write the value
		buf.Write(valueBytes)
	}

	return buf.Bytes(), nil
}

// Decode decompresses the compressed JSON data
func (e *JSONEncoder) Decode(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return [][]byte{}, nil
	}

	reader := bytes.NewReader(data)

	// Read the compression method
	compressionByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	compressionMethod := JSONEncodingFormat(compressionByte)

	// Read the count of JSON objects
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	// Create a slice for the result
	result := make([][]byte, count)

	switch compressionMethod {
	case StandardJSON:
		return e.decodeStandard(reader, result)
	case FieldNameDictionary:
		return e.decodeWithFieldDictionary(reader, result)
	case SchemaOptimized:
		// Not implemented yet, assume field dictionary was used as a fallback
		return e.decodeWithFieldDictionary(reader, result)
	default:
		return nil, fmt.Errorf("unknown JSON compression method: %d", compressionMethod)
	}
}

// decodeStandard decodes standard JSON-encoded data
func (e *JSONEncoder) decodeStandard(reader *bytes.Reader, result [][]byte) ([][]byte, error) {
	for i := 0; i < len(result); i++ {
		// Read object length
		var objLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &objLen); err != nil {
			return nil, err
		}

		// Read the JSON object
		objData := make([]byte, objLen)
		if _, err := reader.Read(objData); err != nil {
			return nil, err
		}

		result[i] = objData
	}

	return result, nil
}

// decodeWithFieldDictionary decodes JSON data that was encoded with a field dictionary
func (e *JSONEncoder) decodeWithFieldDictionary(reader *bytes.Reader, result [][]byte) ([][]byte, error) {
	// Read the dictionary length
	var dictLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &dictLen); err != nil {
		return nil, err
	}

	// Read the dictionary data
	dictData := make([]byte, dictLen)
	if _, err := reader.Read(dictData); err != nil {
		return nil, err
	}

	// Deserialize the dictionary
	dict := newFieldDictionary()
	if err := dict.deserialize(dictData); err != nil {
		return nil, err
	}

	// Decode each JSON object
	for i := 0; i < len(result); i++ {
		// Read encoded object length
		var encodedLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &encodedLen); err != nil {
			return nil, err
		}

		// If length is zero, this is an invalid JSON object
		if encodedLen == 0 {
			result[i] = []byte("null")
			continue
		}

		// Read the encoded object data
		encodedData := make([]byte, encodedLen)
		if _, err := reader.Read(encodedData); err != nil {
			return nil, err
		}

		// Decode the object
		decodedObj, err := e.decodeObject(encodedData, dict)
		if err != nil {
			return nil, err
		}

		// Convert to JSON
		jsonBytes, err := json.Marshal(decodedObj)
		if err != nil {
			return nil, err
		}

		result[i] = jsonBytes
	}

	return result, nil
}

// decodeObject decodes a single encoded object back to a map
func (e *JSONEncoder) decodeObject(encodedData []byte, dict *fieldDictionary) (map[string]interface{}, error) {
	reader := bytes.NewReader(encodedData)

	// Read the field count
	var fieldCount uint16
	if err := binary.Read(reader, binary.LittleEndian, &fieldCount); err != nil {
		return nil, err
	}

	// Create the result object
	result := make(map[string]interface{})

	// Decode each field
	for i := uint16(0); i < fieldCount; i++ {
		// Read field ID
		var fieldID uint16
		if err := binary.Read(reader, binary.LittleEndian, &fieldID); err != nil {
			return nil, err
		}

		// Get field name from dictionary
		fieldName, err := dict.get(fieldID)
		if err != nil {
			return nil, err
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		// Read value data
		valueData := make([]byte, valueLen)
		if _, err := reader.Read(valueData); err != nil {
			return nil, err
		}

		// Parse the value
		var value interface{}
		if err := json.Unmarshal(valueData, &value); err != nil {
			return nil, err
		}

		// Add to result
		result[fieldName] = value
	}

	return result, nil
}
