package fastmap

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// FastInt64Map is a highly optimized map for int64 keys
// This is the production version incorporating all performance optimizations
type FastInt64Map[V any] struct {
	buckets []fastBucket[V]
	mask    uint64
	count   atomic.Int64
}

type fastBucket[V any] struct {
	head unsafe.Pointer // *fastNode[V]
}

type fastNode[V any] struct {
	key     int64
	value   atomic.Pointer[V]
	next    unsafe.Pointer // *fastNode[V]
	deleted uint32
}

// NewFastInt64Map creates a new optimized map for int64 keys
func NewFastInt64Map[V any](sizePower uint) *FastInt64Map[V] {
	if sizePower < 2 {
		sizePower = 2 // Minimum 4 buckets
	}

	size := uint64(1 << sizePower)
	mask := size - 1

	// Create buckets
	buckets := make([]fastBucket[V], size)

	return &FastInt64Map[V]{
		buckets: buckets,
		mask:    mask,
	}
}

// hashFast is an optimized hash function for int64 keys
// that combines speed and excellent distribution
func hashFast(x int64) uint64 {
	key := uint64(x)

	// Fast avalanche function - spreads bits quickly
	// Optimized for modern CPU pipelines
	key = key * 0xd6e8feb86659fd93
	key = bits.RotateLeft64(key, 32) ^ key

	return key
}

// Get retrieves a value by key
func (m *FastInt64Map[V]) Get(key int64) (V, bool) {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// Search in the linked list
	for node := (*fastNode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fastNode[V])(atomic.LoadPointer(&node.next)) {

		if node.key == key && atomic.LoadUint32(&node.deleted) == 0 {
			value := node.value.Load()
			if value != nil {
				return *value, true
			}
			break
		}
	}

	var zero V
	return zero, false
}

// Set adds or updates a key-value pair
func (m *FastInt64Map[V]) Set(key int64, value V) {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// First check if the key already exists
	var predecessor *fastNode[V]
	var current *fastNode[V]

	// Load the head of the list
	head := (*fastNode[V])(atomic.LoadPointer(&bucket.head))

	// Check if the key exists or find insertion point
	for current = head; current != nil; {
		if current.key == key {
			// Key exists, update its value
			if atomic.LoadUint32(&current.deleted) == 0 {
				// Node is not deleted, update value
				current.value.Store(&value)
				return
			} else {
				// Node is deleted, try to resurrect it
				if atomic.CompareAndSwapUint32(&current.deleted, 1, 0) {
					current.value.Store(&value)
					m.count.Add(1)
					return
				}
				// If CAS failed, another thread modified it, retry
				atomic.CompareAndSwapUint32(&current.deleted, 0, 0) // Memory barrier
				current = (*fastNode[V])(atomic.LoadPointer(&bucket.head))
				predecessor = nil
				continue
			}
		}

		predecessor = current
		current = (*fastNode[V])(atomic.LoadPointer(&current.next))
	}

	// Key doesn't exist, create a new node
	newNode := &fastNode[V]{
		key: key,
	}
	newNode.value.Store(&value)

	// Insert at head if first node or predecessor is nil
	if head == nil || predecessor == nil {
		for {
			// Load the current head
			currentHead := (*fastNode[V])(atomic.LoadPointer(&bucket.head))

			// Set the new node's next pointer to the current head
			atomic.StorePointer(&newNode.next, unsafe.Pointer(currentHead))

			// Try to set the new node as the new head
			if atomic.CompareAndSwapPointer(&bucket.head, unsafe.Pointer(currentHead), unsafe.Pointer(newNode)) {
				break
			}
		}
	} else {
		// Insert after predecessor
		for {
			// Load predecessor's next
			next := (*fastNode[V])(atomic.LoadPointer(&predecessor.next))

			// Set new node's next
			atomic.StorePointer(&newNode.next, unsafe.Pointer(next))

			// Try to set predecessor's next to the new node
			if atomic.CompareAndSwapPointer(&predecessor.next, unsafe.Pointer(next), unsafe.Pointer(newNode)) {
				break
			}

			// If CAS failed, retry the entire operation
			current = (*fastNode[V])(atomic.LoadPointer(&bucket.head))
			predecessor = nil

			for current != nil && current.key != key {
				predecessor = current
				current = (*fastNode[V])(atomic.LoadPointer(&current.next))
			}

			if current != nil && current.key == key {
				// Key exists, update instead of insert
				if atomic.LoadUint32(&current.deleted) == 0 {
					current.value.Store(&value)
					return
				} else if atomic.CompareAndSwapUint32(&current.deleted, 1, 0) {
					current.value.Store(&value)
					m.count.Add(1)
					return
				}
				// If we reached here, break out of this loop
				// The outer loop will retry the insertion
				break
			}
		}
	}

	// Increment counter
	m.count.Add(1)
}

// Del removes a key from the map
func (m *FastInt64Map[V]) Del(key int64) bool {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// Search for the key
	for node := (*fastNode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fastNode[V])(atomic.LoadPointer(&node.next)) {

		if node.key == key && atomic.LoadUint32(&node.deleted) == 0 {
			// Found non-deleted node with matching key
			if atomic.CompareAndSwapUint32(&node.deleted, 0, 1) {
				// Successfully marked as deleted
				m.count.Add(-1)
				return true
			}
			// If CAS failed, someone else deleted it or resurrected it
			return false
		}
	}

	// Key not found or already deleted
	return false
}

// Len returns the number of elements in the map
func (m *FastInt64Map[V]) Len() int64 {
	return m.count.Load()
}

// ForEach iterates through all key-value pairs
func (m *FastInt64Map[V]) ForEach(f func(int64, V) bool) {
	// For each bucket
	for i := range m.buckets {
		// Get the bucket
		bucket := &m.buckets[i]

		// Iterate through the linked list
		for node := (*fastNode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fastNode[V])(atomic.LoadPointer(&node.next)) {

			// Skip deleted nodes
			if atomic.LoadUint32(&node.deleted) != 0 {
				continue
			}

			// Get value
			value := node.value.Load()
			if value == nil {
				continue
			}

			// Call the function
			if !f(node.key, *value) {
				return
			}
		}
	}
}

// GetUnderlyingMap returns the underlying map if it's directly backed by a map
// This is used for identity comparison in the scanner to detect if a row map
// is a version store map. Returns (nil, false) if not available/applicable.
func (m *FastInt64Map[V]) GetUnderlyingMap() (map[int64]V, bool) {
	// This is not a real implementation since FastInt64Map doesn't use a map internally
	// It's just a utility method to support the scanner's optimization checks
	return nil, false
}
