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
)

// DictionaryCompressor implements the Compressor interface for dictionary encoding
type DictionaryCompressor struct {
	encoder *DictionaryEncoder
}

// NewDictionaryCompressor creates a new dictionary compressor
func NewDictionaryCompressor() *DictionaryCompressor {
	return &DictionaryCompressor{
		encoder: NewDictionaryEncoder(),
	}
}

// Type returns the compression type
func (c *DictionaryCompressor) Type() CompressionType {
	return Dictionary
}

// Compress compresses a byte slice containing string data
func (c *DictionaryCompressor) Compress(data []byte) ([]byte, error) {
	// Decode the input data format (expects a format where strings are
	// separated by null bytes or prefixed with length)
	strings, err := DecodeStringData(data)
	if err != nil {
		return nil, err
	}

	// Compress the strings using the dictionary encoder
	encodedData, err := c.encoder.CompressStringBlock(strings)
	if err != nil {
		return nil, err
	}

	// Serialize the dictionary
	dictionaryData, err := c.encoder.SerializeDictionary()
	if err != nil {
		return nil, err
	}

	// Combine the dictionary and encoded data
	var buf bytes.Buffer

	// Write the size of the dictionary data
	dictSize := uint32(len(dictionaryData))
	if err := binary.Write(&buf, binary.LittleEndian, dictSize); err != nil {
		return nil, err
	}

	// Write the dictionary data
	if _, err := buf.Write(dictionaryData); err != nil {
		return nil, err
	}

	// Write the encoded data
	if _, err := buf.Write(encodedData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress decompresses dictionary-encoded data
func (c *DictionaryCompressor) Decompress(data []byte) ([]byte, error) {
	buf := bytes.NewReader(data)

	// Read the size of the dictionary data
	var dictSize uint32
	if err := binary.Read(buf, binary.LittleEndian, &dictSize); err != nil {
		return nil, err
	}

	// Read the dictionary data
	dictionaryData := make([]byte, dictSize)
	if _, err := buf.Read(dictionaryData); err != nil {
		return nil, err
	}

	// Deserialize the dictionary
	if err := c.encoder.DeserializeDictionary(dictionaryData); err != nil {
		return nil, err
	}

	// Read the remaining data (encoded values)
	encodedData := make([]byte, buf.Len())
	if _, err := buf.Read(encodedData); err != nil {
		return nil, err
	}

	// Decompress the encoded data
	strings, err := c.encoder.DecompressStringBlock(encodedData)
	if err != nil {
		return nil, err
	}

	// Encode the strings back to the original format
	return EncodeStringData(strings), nil
}

// DecodeStringData decodes a byte slice into a slice of strings
// This function assumes the data is in a simple format where each string
// is prefixed with its length as a uint32
func DecodeStringData(data []byte) ([]string, error) {
	if len(data) == 0 {
		return []string{}, nil
	}

	buf := bytes.NewReader(data)

	// Read the number of strings
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	strings := make([]string, count)

	// Read each string
	for i := uint32(0); i < count; i++ {
		// Read string length
		var strLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}

		// Read string data
		strData := make([]byte, strLen)
		if _, err := buf.Read(strData); err != nil {
			return nil, err
		}

		strings[i] = string(strData)
	}

	return strings, nil
}

// EncodeStringData encodes a slice of strings into a byte slice
// This function uses a simple format where each string is prefixed with its length as a uint32
func EncodeStringData(strings []string) []byte {
	var buf bytes.Buffer

	// Write the number of strings
	count := uint32(len(strings))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each string
	for _, s := range strings {
		// Write string length
		strLen := uint32(len(s))
		binary.Write(&buf, binary.LittleEndian, strLen)

		// Write string data
		buf.Write([]byte(s))
	}

	return buf.Bytes()
}
