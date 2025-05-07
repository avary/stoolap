// Package fastmap contains a fast Int64Map implementation for int64 keys.
// Copyright (c) 2016, Brent Pedersen - Bioinformatics
package fastmap

import (
	"iter"
	"math"
)

type pair[Value any] struct {
	Key   int64
	Value Value
}

const fillFactor64 = 0.75

// Methods valid on a nil Int64Map are Has, Get, Len, and ForEach.
type Int64Map[Value any] struct {
	data []pair[Value] // key-value pairs
	size int

	zeroVal    Value // value of 'zero' key
	hasZeroKey bool  // do we have 'zero' key in the Int64Map?
}

// New creates a new Int64Map with keys being any integer subtype.
// The Int64Map can store up to the given capacity before reallocation and rehashing occurs.
func NewInt64Map[V any](capacity int) *Int64Map[V] {
	return &Int64Map[V]{
		data: make([]pair[V], arraySize(capacity, fillFactor64)),
	}
}

// Has checks if the given key exists in the Int64Map.
// Calling this method on a nil Int64Map will return false.
func (m *Int64Map[V]) Has(key int64) bool {
	if m == nil {
		return false
	}

	if key == int64(0) {
		return m.hasZeroKey
	}

	idx := m.startIndex(key)
	p := m.data[idx]

	if p.Key == int64(0) { // end of chain already
		return false
	}
	if p.Key == key { // we check zero prior to this call
		return true
	}

	// hash collision, seek next hash match, bailing on first empty
	for {
		idx = m.nextIndex(idx)
		p = m.data[idx]
		if p.Key == int64(0) {
			return false
		}
		if p.Key == key {
			return true
		}
	}
}

// Get returns the value if the key is found.
// If you just need to check for existence it is easier to use Has.
// Calling this method on a nil Int64Map will return the zero value for V and false.
func (m *Int64Map[V]) Get(key int64) (V, bool) {
	if m == nil {
		var zero V
		return zero, false
	}

	if key == int64(0) {
		if m.hasZeroKey {
			return m.zeroVal, true
		}
		var zero V
		return zero, false
	}

	idx := m.startIndex(key)
	p := m.data[idx]

	if p.Key == int64(0) { // end of chain already
		var zero V
		return zero, false
	}
	if p.Key == key { // we check zero prior to this call
		return p.Value, true
	}

	// hash collision, seek next hash match, bailing on first empty
	for {
		idx = m.nextIndex(idx)
		p = m.data[idx]
		if p.Key == int64(0) {
			var zero V
			return zero, false
		}
		if p.Key == key {
			return p.Value, true
		}
	}
}

// Put adds or updates key with value val.
func (m *Int64Map[V]) Put(key int64, val V) {
	if key == int64(0) {
		if !m.hasZeroKey {
			m.size++
		}
		m.zeroVal = val
		m.hasZeroKey = true
		return
	}

	idx := m.startIndex(key)
	p := &m.data[idx]

	if p.Key == int64(0) { // end of chain already
		p.Key = key
		p.Value = val
		if m.size >= m.sizeThreshold() {
			m.rehash()
		} else {
			m.size++
		}
		return
	} else if p.Key == key { // overwrite existing value
		p.Value = val
		return
	}

	// hash collision, seek next empty or key match
	for {
		idx = m.nextIndex(idx)
		p = &m.data[idx]

		if p.Key == int64(0) {
			p.Key = key
			p.Value = val
			if m.size >= m.sizeThreshold() {
				m.rehash()
			} else {
				m.size++
			}
			return
		} else if p.Key == key {
			p.Value = val
			return
		}
	}
}

// PutIfNotExists adds the key-value pair only if the key does not already exist
// in the Int64Map, and returns the current value associated with the key and a boolean
// indicating whether the value was newly added or not.
func (m *Int64Map[V]) PutIfNotExists(key int64, val V) (V, bool) {
	if key == int64(0) {
		if m.hasZeroKey {
			return m.zeroVal, false
		}
		m.zeroVal = val
		m.hasZeroKey = true
		m.size++
		return val, true
	}

	idx := m.startIndex(key)
	p := &m.data[idx]

	if p.Key == int64(0) { // end of chain already
		p.Key = key
		p.Value = val
		m.size++
		if m.size >= m.sizeThreshold() {
			m.rehash()
		}
		return val, true
	} else if p.Key == key {
		return p.Value, false
	}

	// hash collision, seek next hash match, bailing on first empty
	for {
		idx = m.nextIndex(idx)
		p = &m.data[idx]

		if p.Key == int64(0) {
			p.Key = key
			p.Value = val
			m.size++
			if m.size >= m.sizeThreshold() {
				m.rehash()
			}
			return val, true
		} else if p.Key == key {
			return p.Value, false
		}
	}
}

// ForEach iterates through key-value pairs in the Int64Map while the function f returns true.
// This method returns immediately if invoked on a nil Int64Map.
//
// The iteration order of a Int64Map is not defined, so please avoid relying on it.
func (m *Int64Map[V]) ForEach(f func(int64, V) bool) {
	if m == nil {
		return
	}

	if m.hasZeroKey && !f(int64(0), m.zeroVal) {
		return
	}

	for _, p := range m.data {
		if p.Key != int64(0) && !f(p.Key, p.Value) {
			return
		}
	}
}

// All returns an iterator over key-value pairs from m.
// The iterator returns immediately if invoked on a nil Int64Map.
//
// The iteration order of a Int64Map is not defined, so please avoid relying on it.
func (m *Int64Map[V]) All() iter.Seq2[int64, V] {
	return m.ForEach
}

// Keys returns an iterator over keys in m.
// The iterator returns immediately if invoked on a nil Int64Map.
//
// The iteration order of a Int64Map is not defined, so please avoid relying on it.
func (m *Int64Map[V]) Keys() iter.Seq[int64] {
	return func(yield func(k int64) bool) {
		if m == nil {
			return
		}

		if m.hasZeroKey && !yield(int64(0)) {
			return
		}

		for _, p := range m.data {
			if p.Key != int64(0) && !yield(p.Key) {
				return
			}
		}
	}
}

// Values returns an iterator over values in m.
// The iterator returns immediately if invoked on a nil Int64Map.
//
// The iteration order of a Int64Map is not defined, so please avoid relying on it.
func (m *Int64Map[V]) Values() iter.Seq[V] {
	return func(yield func(v V) bool) {
		if m == nil {
			return
		}

		if m.hasZeroKey && !yield(m.zeroVal) {
			return
		}

		for _, p := range m.data {
			if p.Key != int64(0) && !yield(p.Value) {
				return
			}
		}
	}
}

// Clear removes all items from the Int64Map, but keeps the internal buffers for reuse.
func (m *Int64Map[V]) Clear() {
	var zero V
	m.hasZeroKey = false
	m.zeroVal = zero

	// compiles down to runtime.memclr()
	for i := range m.data {
		m.data[i] = pair[V]{}
	}

	m.size = 0
}

func (m *Int64Map[V]) rehash() {
	oldData := m.data
	m.data = make([]pair[V], 2*len(m.data))

	// reset size
	if m.hasZeroKey {
		m.size = 1
	} else {
		m.size = 0
	}

	forEach64(oldData, func(k int64, v V) bool {
		m.Put(k, v)
		return true
	})
}

// Len returns the number of elements in the Int64Map.
// The length of a nil Int64Map is defined to be zero.
func (m *Int64Map[V]) Len() int {
	if m == nil {
		return 0
	}

	return m.size
}

func (m *Int64Map[V]) sizeThreshold() int {
	return int(math.Floor(float64(len(m.data)) * fillFactor64))
}

func (m *Int64Map[V]) startIndex(key int64) int {
	h := hashFast(key)
	return int(h) & (len(m.data) - 1)
}

func (m *Int64Map[V]) nextIndex(idx int) int {
	return (idx + 1) & (len(m.data) - 1)
}

func forEach64[V any](pairs []pair[V], f func(k int64, v V) bool) {
	for _, p := range pairs {
		if p.Key != int64(0) && !f(p.Key, p.Value) {
			return
		}
	}
}

// Del deletes a key and its value, returning true iff the key was found
func (m *Int64Map[V]) Del(key int64) bool {
	if key == int64(0) {
		if m.hasZeroKey {
			m.hasZeroKey = false
			m.size--
			return true
		}
		return false
	}

	idx := m.startIndex(key)
	p := m.data[idx]

	if p.Key == key {
		// any keys that were pushed back needs to be shifted nack into the empty slot
		// to avoid breaking the chain
		m.shiftKeys(idx)
		m.size--
		return true
	} else if p.Key == int64(0) { // end of chain already
		return false
	}

	for {
		idx = m.nextIndex(idx)
		p = m.data[idx]

		if p.Key == key {
			// any keys that were pushed back needs to be shifted nack into the empty slot
			// to avoid breaking the chain
			m.shiftKeys(idx)
			m.size--
			return true
		} else if p.Key == int64(0) {
			return false
		}

	}
}

func (m *Int64Map[V]) shiftKeys(idx int) int {
	// Shift entries with the same hash.
	// We need to do this on deletion to ensure we don't have zeroes in the hash chain
	for {
		var p pair[V]
		lastIdx := idx
		idx = m.nextIndex(idx)
		for {
			p = m.data[idx]
			if p.Key == int64(0) {
				m.data[lastIdx] = pair[V]{}
				return lastIdx
			}

			slot := m.startIndex(p.Key)
			if lastIdx <= idx {
				if lastIdx >= slot || slot > idx {
					break
				}
			} else {
				if lastIdx >= slot && slot > idx {
					break
				}
			}
			idx = m.nextIndex(idx)
		}
		m.data[lastIdx] = p
	}
}

func nextPowerOf2(x uint32) uint32 {
	if x == math.MaxUint32 {
		return x
	}

	if x == 0 {
		return 1
	}

	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16

	return x + 1
}

func arraySize(exp int, fill float64) int {
	s := nextPowerOf2(uint32(math.Ceil(float64(exp) / fill)))
	if s < 2 {
		s = 2
	}
	return int(s)
}
