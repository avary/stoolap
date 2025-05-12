package fastmap

import (
	"iter"
	"sync"
	"sync/atomic"
)

// SegmentInt64Map is a fast, thread-safe map for int64 keys
// that uses sharding (segmentation) to reduce lock contention
// while maintaining the performance benefits of Int64Map
type SegmentInt64Map[V any] struct {
	segments    []*segment[V]
	segmentMask int
	segmentBits uint8
	count       atomic.Int64
}

// segment is a single shard of the map
type segment[V any] struct {
	data   *Int64Map[V] // Use the optimized Int64Map internally
	rwlock sync.RWMutex // RWMutex for fine-grained locking
}

// NewSegmentInt64Map creates a new segmented map for int64 keys
// segmentPower controls the number of segments (2^segmentPower)
// initialCapacity is the initial capacity per segment
func NewSegmentInt64Map[V any](segmentPower uint8, initialCapacity int) *SegmentInt64Map[V] {
	// Ensure segment power is at least 4 (16 segments) and at most 8 (256 segments)
	if segmentPower < 4 {
		segmentPower = 4 // Minimum 16 segments
	} else if segmentPower > 8 {
		segmentPower = 8 // Maximum 256 segments
	}

	segmentCount := 1 << segmentPower
	segmentMask := segmentCount - 1

	// Calculate per-segment capacity
	segmentCapacity := initialCapacity / segmentCount
	if segmentCapacity < 8 {
		segmentCapacity = 8 // Minimum per-segment capacity
	}

	// Create segments
	segments := make([]*segment[V], segmentCount)
	for i := 0; i < segmentCount; i++ {
		segments[i] = &segment[V]{
			data: NewInt64Map[V](segmentCapacity),
		}
	}

	return &SegmentInt64Map[V]{
		segments:    segments,
		segmentMask: segmentMask,
		segmentBits: segmentPower,
		// count is initialized to 0 by default
	}
}

// getSegment returns the segment for a given key
func (m *SegmentInt64Map[V]) getSegment(key int64) *segment[V] {
	// Use the high bits of the hash as segment index
	// This distributes sequential keys across different segments
	// and reduces contention for clustered keys
	h := uint(key * int64(0x9E3779B9))
	segmentIndex := (h >> 16) & uint(m.segmentMask)
	return m.segments[segmentIndex]
}

// Has checks if a key exists in the map
func (m *SegmentInt64Map[V]) Has(key int64) bool {
	segment := m.getSegment(key)
	segment.rwlock.RLock()
	defer segment.rwlock.RUnlock()
	return segment.data.Has(key)
}

// Get retrieves a value by key
func (m *SegmentInt64Map[V]) Get(key int64) (V, bool) {
	segment := m.getSegment(key)
	segment.rwlock.RLock()
	defer segment.rwlock.RUnlock()
	return segment.data.Get(key)
}

// Set adds or updates a key-value pair
func (m *SegmentInt64Map[V]) Set(key int64, value V) {
	segment := m.getSegment(key)
	segment.rwlock.Lock()
	defer segment.rwlock.Unlock()

	// Check if we're adding a new key (for accurate count)
	exists := segment.data.Has(key)
	segment.data.Put(key, value)

	// Only increment count if it's a new key
	if !exists {
		m.count.Add(1)
	}
}

// PutIfNotExists adds the key-value pair only if the key doesn't already exist
// Returns the value and true if inserted, or existing value and false if not inserted
func (m *SegmentInt64Map[V]) PutIfNotExists(key int64, value V) (V, bool) {
	segment := m.getSegment(key)
	segment.rwlock.Lock()
	defer segment.rwlock.Unlock()

	result, inserted := segment.data.PutIfNotExists(key, value)
	if inserted {
		m.count.Add(1)
	}
	return result, inserted
}

// Del removes a key from the map
func (m *SegmentInt64Map[V]) Del(key int64) bool {
	segment := m.getSegment(key)
	segment.rwlock.Lock()
	defer segment.rwlock.Unlock()

	deleted := segment.data.Del(key)
	if deleted {
		m.count.Add(-1)
	}
	return deleted
}

// Len returns the number of elements in the map
func (m *SegmentInt64Map[V]) Len() int64 {
	return m.count.Load()
}

// ForEach iterates through all key-value pairs
// Note: The iteration is not atomic and may miss concurrent updates
func (m *SegmentInt64Map[V]) ForEach(f func(int64, V) bool) {
	// For each segment
	for _, segment := range m.segments {
		// Lock segment for reading
		segment.rwlock.RLock()

		// Create a copy of the callback to avoid capturing the loop variable
		callback := f
		continueIteration := true

		// Use the segment's map ForEach with a wrapper that manages continuation
		segment.data.ForEach(func(key int64, value V) bool {
			result := callback(key, value)
			if !result {
				continueIteration = false
			}
			return result
		})

		// Unlock segment
		segment.rwlock.RUnlock()

		// Check if we should stop iteration
		if !continueIteration {
			break
		}
	}
}

// All returns an iterator over all key-value pairs
func (m *SegmentInt64Map[V]) All() iter.Seq2[int64, V] {
	return m.ForEach
}

// Keys returns an iterator over all keys
func (m *SegmentInt64Map[V]) Keys() iter.Seq[int64] {
	return func(yield func(int64) bool) {
		m.ForEach(func(key int64, _ V) bool {
			return yield(key)
		})
	}
}

// Values returns an iterator over all values
func (m *SegmentInt64Map[V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.ForEach(func(_ int64, value V) bool {
			return yield(value)
		})
	}
}

// Clear removes all entries from the map
func (m *SegmentInt64Map[V]) Clear() {
	// For each segment
	for _, segment := range m.segments {
		segment.rwlock.Lock()
		segment.data.Clear()
		segment.rwlock.Unlock()
	}

	// Reset count
	m.count.Store(0)
}
