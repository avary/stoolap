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

// Package bitmap implements bitmap indexes for low-cardinality columns.
//
// The bitmap data structure provides thread-safe operations through proper
// synchronization. This enables safe concurrent access during both index
// creation/updates and query operations. The implementation is fully
// integrated with the storage engine for efficient query execution.
package bitmap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"sync"
)

// Bitmap represents a bitmap index for a column
type Bitmap struct {
	// data is the bitmap data stored as a slice of uint64 words
	data []uint64
	// cardinality is the number of bits set to 1
	cardinality int64
	// size is the total number of bits in the bitmap
	size int64
	// mutex protects concurrent access to the bitmap data
	mutex sync.RWMutex
}

// New creates a new bitmap with the specified size (in bits)
func New(size int64) *Bitmap {
	// Calculate the number of uint64 words needed
	wordCount := (size + 63) / 64
	return &Bitmap{
		data:        make([]uint64, wordCount),
		cardinality: 0,
		size:        size,
	}
}

// NewFromData creates a bitmap from existing data
func NewFromData(data []uint64, size int64) *Bitmap {
	bm := &Bitmap{
		data: data,
		size: size,
	}

	// Calculate cardinality
	bm.cardinality = 0
	for _, word := range data {
		bm.cardinality += int64(bits.OnesCount64(word))
	}

	return bm
}

// Set sets the bit at the specified position to 1 or 0 based on the value parameter
func (bm *Bitmap) Set(pos int64, value bool) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if pos < 0 || pos >= bm.size {
		return errors.New("position out of range")
	}

	wordIdx := pos / 64
	bitPos := pos % 64

	if value {
		// Setting to 1
		// Check if bit is already set
		if (bm.data[wordIdx] & (1 << bitPos)) == 0 {
			bm.data[wordIdx] |= 1 << bitPos
			bm.cardinality++
		}
	} else {
		// Setting to 0
		// Check if bit is already cleared
		if (bm.data[wordIdx] & (1 << bitPos)) != 0 {
			bm.data[wordIdx] &= ^(1 << bitPos)
			bm.cardinality--
		}
	}

	return nil
}

// Clear sets the bit at the specified position to 0
func (bm *Bitmap) Clear(pos int64) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if pos < 0 || pos >= bm.size {
		return errors.New("position out of range")
	}

	wordIdx := pos / 64
	bitPos := pos % 64

	// Check if bit is already cleared
	if (bm.data[wordIdx] & (1 << bitPos)) != 0 {
		bm.data[wordIdx] &= ^(1 << bitPos)
		bm.cardinality--
	}

	return nil
}

// Get returns the bit value at the specified position
func (bm *Bitmap) Get(pos int64) (bool, error) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	if pos < 0 || pos >= bm.size {
		return false, errors.New("position out of range")
	}

	wordIdx := pos / 64
	bitPos := pos % 64

	return (bm.data[wordIdx] & (1 << bitPos)) != 0, nil
}

// Cardinality returns the number of bits set to 1
func (bm *Bitmap) Cardinality() int64 {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	return bm.cardinality
}

// CountSetBits returns the number of bits set to 1
// This is an alias for Cardinality() for API consistency
func (bm *Bitmap) CountSetBits() int64 {
	return bm.Cardinality()
}

// Size returns the total size of the bitmap in bits
func (bm *Bitmap) Size() int64 {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	return bm.size
}

// And performs bitwise AND operation with another bitmap
func (bm *Bitmap) And(other *Bitmap) (*Bitmap, error) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	if bm.size != other.size {
		return nil, errors.New("bitmap sizes do not match")
	}

	result := make([]uint64, len(bm.data))
	for i := range bm.data {
		result[i] = bm.data[i] & other.data[i]
	}

	return NewFromData(result, bm.size), nil
}

// Or performs bitwise OR operation with another bitmap
func (bm *Bitmap) Or(other *Bitmap) (*Bitmap, error) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	if bm.size != other.size {
		return nil, errors.New("bitmap sizes do not match")
	}

	result := make([]uint64, len(bm.data))
	for i := range bm.data {
		result[i] = bm.data[i] | other.data[i]
	}

	return NewFromData(result, bm.size), nil
}

// Not performs bitwise NOT operation on the bitmap
func (bm *Bitmap) Not() *Bitmap {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	result := make([]uint64, len(bm.data))

	for i := range bm.data {
		result[i] = ^bm.data[i]
	}

	// Clear any bits beyond the bitmap size
	if bm.size%64 != 0 {
		lastWordIdx := len(result) - 1
		extraBits := 64 - (bm.size % 64)
		result[lastWordIdx] &= ^(^uint64(0) << (64 - extraBits))
	}

	return NewFromData(result, bm.size)
}

// AndNot performs bitwise AND NOT operation (A & ~B)
func (bm *Bitmap) AndNot(other *Bitmap) (*Bitmap, error) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	if bm.size != other.size {
		return nil, errors.New("bitmap sizes do not match")
	}

	result := make([]uint64, len(bm.data))
	for i := range bm.data {
		result[i] = bm.data[i] & ^other.data[i]
	}

	return NewFromData(result, bm.size), nil
}

// ToByteSlice converts the bitmap to a byte slice
func (bm *Bitmap) ToByteSlice() []byte {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	buf := new(bytes.Buffer)

	// Write size
	binary.Write(buf, binary.LittleEndian, int64(bm.size))

	// Write data
	for _, word := range bm.data {
		binary.Write(buf, binary.LittleEndian, word)
	}

	return buf.Bytes()
}

// FromByteSlice restores a bitmap from a byte slice
func FromByteSlice(data []byte) (*Bitmap, error) {
	buf := bytes.NewReader(data)

	// Read size
	var size int64
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	// Calculate word count
	wordCount := (int(size) + 63) / 64
	words := make([]uint64, wordCount)

	// Read data
	for i := 0; i < wordCount; i++ {
		var word uint64
		if err := binary.Read(buf, binary.LittleEndian, &word); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		words[i] = word
	}

	return NewFromData(words, size), nil
}

// Iterate calls the specified function for each position where the bit is set to 1
func (bm *Bitmap) Iterate(fn func(pos int64) bool) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	for wordIdx, word := range bm.data {
		if word == 0 {
			continue
		}

		basePos := wordIdx * 64

		for bitPos := 0; bitPos < 64; bitPos++ {
			if (word & (1 << bitPos)) != 0 {
				pos := basePos + bitPos
				if int64(pos) >= bm.size {
					break
				}

				if !fn(int64(pos)) {
					return
				}
			}
		}
	}
}

// Clone creates a deep copy of the bitmap
func (bm *Bitmap) Clone() *Bitmap {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	// Create new data slice with same capacity
	newData := make([]uint64, len(bm.data))

	// Copy all words
	copy(newData, bm.data)

	// Create and return new bitmap with copied data
	return &Bitmap{
		data:        newData,
		cardinality: bm.cardinality,
		size:        bm.size,
	}
}

// Resize resizes the bitmap to the specified size
func (bm *Bitmap) Resize(newSize int64) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if newSize <= bm.size {
		// If shrinking, just update the size
		bm.size = newSize
		return
	}

	// Calculate how many uint64 words we need
	newWordCount := (newSize + 63) / 64
	currentWordCount := int64(len(bm.data))

	// If we need more words, expand the slice
	if newWordCount > currentWordCount {
		// Create new data slice
		newData := make([]uint64, newWordCount)

		// Copy existing data
		copy(newData, bm.data)

		// Update data slice
		bm.data = newData
	}

	// Update size
	bm.size = newSize
}

// SetAll sets all bits in the bitmap to the specified value
func (bm *Bitmap) SetAll(value bool) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	fillValue := uint64(0)
	if value {
		fillValue = ^uint64(0) // All 1s
	}

	// Fill all full words
	for i := range bm.data {
		bm.data[i] = fillValue
	}

	// Handle partial last word if necessary
	if value && bm.size%64 != 0 {
		// Clear extra bits in the last word
		lastWordIdx := len(bm.data) - 1
		extraBits := 64 - (bm.size % 64)
		bm.data[lastWordIdx] &= ^(^uint64(0) << (64 - extraBits))
	}

	// Update cardinality
	if value {
		bm.cardinality = bm.size
	} else {
		bm.cardinality = 0
	}
}
