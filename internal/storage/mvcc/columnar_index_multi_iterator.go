// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"sync"

	"github.com/stoolap/stoolap/internal/btree"
	"github.com/stoolap/stoolap/internal/fastmap"
)

// MultiColumnIterator provides a high-performance iterator over the MultiColumnarIndex
// This pattern avoids the allocation costs of callbacks and allows early termination
type MultiColumnIterator struct {
	// The values tree being iterated
	tree *btree.BTree[MultiColumnValue, []int64]

	// The current iterator state
	iterator *btree.Iterator[MultiColumnValue, []int64]

	// The current key-value pair
	currentKey    MultiColumnValue
	currentRowIDs []int64

	// Current position in the row IDs slice
	currentRowIdx int

	// Reusable buffers to avoid allocations
	seen *fastmap.Int64Map[struct{}]

	// Status tracking
	valid bool

	// Mutex for thread safety
	mutex *sync.RWMutex
}

// NewMultiColumnIterator creates a new iterator for the provided tree
func NewMultiColumnIterator(tree *btree.BTree[MultiColumnValue, []int64], mutex *sync.RWMutex) *MultiColumnIterator {
	mutex.RLock() // Read lock on the tree

	iter := &MultiColumnIterator{
		tree:     tree,
		iterator: tree.Iterate(),
		seen:     int64StructMapPool.Get().(*fastmap.Int64Map[struct{}]),
		mutex:    mutex,
		valid:    false,
	}

	// Position at the first entry
	iter.Next()
	return iter
}

// Next advances the iterator to the next entry
func (it *MultiColumnIterator) Next() bool {
	// If we're still in the current row IDs slice, just advance the index
	if it.currentRowIDs != nil && it.currentRowIdx < len(it.currentRowIDs)-1 {
		it.currentRowIdx++
		it.valid = true
		return true
	}

	// Otherwise, get the next key-value pair from the tree
	if it.iterator.Next() {
		it.currentKey, it.currentRowIDs = it.iterator.Get()
		it.currentRowIdx = 0
		it.valid = len(it.currentRowIDs) > 0
		return it.valid
	}

	// No more entries
	it.valid = false
	return false
}

// Key returns the current key
func (it *MultiColumnIterator) Key() MultiColumnValue {
	if !it.valid {
		return nil
	}
	return it.currentKey
}

// RowID returns the current row ID
func (it *MultiColumnIterator) RowID() int64 {
	if !it.valid || it.currentRowIDs == nil || it.currentRowIdx >= len(it.currentRowIDs) {
		return -1
	}
	return it.currentRowIDs[it.currentRowIdx]
}

// Valid returns whether the iterator is positioned at a valid entry
func (it *MultiColumnIterator) Valid() bool {
	return it.valid
}

// Close releases the iterator and any resources it holds
func (it *MultiColumnIterator) Close() {
	if it.mutex != nil {
		it.mutex.RUnlock()
		it.mutex = nil
	}

	it.tree = nil
	it.iterator = nil
	it.currentKey = nil
	it.currentRowIDs = nil

	// Return the seen map to the pool
	if it.seen != nil {
		it.seen.Clear()
		int64StructMapPool.Put(it.seen)
		it.seen = nil
	}

	it.valid = false
}

// Reset repositions the iterator at the beginning
func (it *MultiColumnIterator) Reset() {
	// Clear the seen map
	it.seen.Clear()

	// Reset the iterator
	it.iterator = it.tree.Iterate()
	it.currentKey = nil
	it.currentRowIDs = nil
	it.currentRowIdx = 0

	// Position at the first entry
	it.Next()
}

// SeekKey positions the iterator at the first entry with a key >= the provided key
func (it *MultiColumnIterator) SeekKey(key MultiColumnValue) bool {
	// Get a new iterator starting at or after the target key
	it.iterator = it.tree.SeekGE(key)

	if it.iterator.Valid() {
		it.currentKey, it.currentRowIDs = it.iterator.Get()
		it.currentRowIdx = 0
		it.valid = len(it.currentRowIDs) > 0
		return it.valid
	}

	it.valid = false
	return false
}

// ForEachRowID applies a function to each row ID associated with the current key
// This avoids the allocation of a full slice when processing row IDs
func (it *MultiColumnIterator) ForEachRowID(fn func(rowID int64) bool) bool {
	if !it.valid || it.currentRowIDs == nil {
		return false
	}

	for i := it.currentRowIdx; i < len(it.currentRowIDs); i++ {
		rowID := it.currentRowIDs[i]
		if !fn(rowID) {
			return false
		}
	}

	return true
}

// MarkSeen marks a row ID as seen in the deduplication map
func (it *MultiColumnIterator) MarkSeen(rowID int64) {
	it.seen.Put(rowID, struct{}{})
}

// HasSeen checks if a row ID has been seen before
func (it *MultiColumnIterator) HasSeen(rowID int64) bool {
	return it.seen.Has(rowID)
}

// RangeIterator provides a specialized iterator for range queries
type RangeIterator struct {
	*MultiColumnIterator

	// Range boundaries
	minKey, maxKey         MultiColumnValue
	includeMin, includeMax bool
}

// NewRangeIterator creates an iterator for a specific value range
func NewRangeIterator(
	tree *btree.BTree[MultiColumnValue, []int64],
	mutex *sync.RWMutex,
	minKey, maxKey MultiColumnValue,
	includeMin, includeMax bool) *RangeIterator {

	iter := &RangeIterator{
		MultiColumnIterator: NewMultiColumnIterator(tree, mutex),
		minKey:              minKey,
		maxKey:              maxKey,
		includeMin:          includeMin,
		includeMax:          includeMax,
	}

	// Position at the first entry in range
	if minKey != nil {
		iter.SeekKey(minKey)

		// Check if we need to skip the first entry (if !includeMin and key == minKey)
		if !includeMin && iter.Valid() {
			key := iter.Key()
			if key.Compare(minKey) == 0 {
				iter.Next()
			}
		}
	} else {
		// Start from the beginning if no min key
		iter.Reset()
	}

	// Ensure the entry is within range
	iter.checkInRange()

	return iter
}

// Next advances to the next entry within the range
func (it *RangeIterator) Next() bool {
	if !it.MultiColumnIterator.Next() {
		return false
	}

	// Check if the entry is within range
	return it.checkInRange()
}

// checkInRange verifies that the current entry is within the range
// Returns true if in range, false otherwise
func (it *RangeIterator) checkInRange() bool {
	if !it.Valid() {
		return false
	}

	key := it.Key()

	// Check if key >= minKey (or > minKey if !includeMin)
	if it.minKey != nil {
		cmp := key.Compare(it.minKey)
		if cmp < 0 || (cmp == 0 && !it.includeMin) {
			it.valid = false
			return false
		}
	}

	// Check if key <= maxKey (or < maxKey if !includeMax)
	if it.maxKey != nil {
		cmp := key.Compare(it.maxKey)
		if cmp > 0 || (cmp == 0 && !it.includeMax) {
			it.valid = false
			return false
		}
	}

	return true
}

// PrefixIterator provides a specialized iterator for prefix matching
type PrefixIterator struct {
	*MultiColumnIterator

	// The prefix to match
	prefix MultiColumnValue
}

// NewPrefixIterator creates an iterator that matches a prefix
func NewPrefixIterator(
	tree *btree.BTree[MultiColumnValue, []int64],
	mutex *sync.RWMutex,
	prefix MultiColumnValue) *PrefixIterator {

	iter := &PrefixIterator{
		MultiColumnIterator: NewMultiColumnIterator(tree, mutex),
		prefix:              prefix,
	}

	// Position at the first entry with the prefix
	if len(prefix) > 0 {
		iter.SeekKey(prefix)
	} else {
		iter.Reset()
	}

	// Ensure the entry has the correct prefix
	iter.checkPrefix()

	return iter
}

// Next advances to the next entry with the matching prefix
func (it *PrefixIterator) Next() bool {
	if !it.MultiColumnIterator.Next() {
		return false
	}

	// Check if the entry has the correct prefix
	return it.checkPrefix()
}

// checkPrefix verifies that the current entry has the specified prefix
// Returns true if it matches, false otherwise
func (it *PrefixIterator) checkPrefix() bool {
	if !it.Valid() {
		return false
	}

	// Check if the current key starts with the prefix
	if len(it.prefix) > 0 {
		key := it.Key()
		if !key.StartsWithPrefix(it.prefix) {
			it.valid = false
			return false
		}
	}

	return true
}

// FilterIterator provides a specialized iterator for filtering entries with a custom function
type FilterIterator struct {
	*MultiColumnIterator

	// Filter function that validates each entry
	isValid func(key MultiColumnValue) bool
}

// NewFilterIterator creates an iterator with a custom filter function
func NewFilterIterator(
	tree *btree.BTree[MultiColumnValue, []int64],
	mutex *sync.RWMutex,
	filterFn func(key MultiColumnValue) bool) *FilterIterator {

	iter := &FilterIterator{
		MultiColumnIterator: NewMultiColumnIterator(tree, mutex),
		isValid:             filterFn,
	}

	// Position at the first valid entry
	iter.findNextValid()

	return iter
}

// Next advances to the next valid entry
func (it *FilterIterator) Next() bool {
	if !it.MultiColumnIterator.Next() {
		return false
	}

	// Find the next valid entry
	return it.findNextValid()
}

// findNextValid advances until a valid entry is found
// Returns true if a valid entry is found, false otherwise
func (it *FilterIterator) findNextValid() bool {
	for it.Valid() {
		key := it.Key()

		// Check if the key passes the filter
		if it.isValid(key) {
			return true
		}

		// Try the next entry
		if !it.MultiColumnIterator.Next() {
			return false
		}
	}

	return false
}

// ColumnFilterIterator is a specialized iterator for filtering by column value
// This is optimized for the common case of filtering on a specific column with a predicate
type ColumnFilterIterator struct {
	*FilterIterator

	// Column index being filtered
	columnIndex int

	// The predicate function for the column
	predicate func(value interface{}) bool
}

// NewColumnFilterIterator creates an iterator that filters based on a column predicate
func NewColumnFilterIterator(
	tree *btree.BTree[MultiColumnValue, []int64],
	mutex *sync.RWMutex,
	columnIndex int,
	predicate func(value interface{}) bool) *ColumnFilterIterator {

	iter := &ColumnFilterIterator{
		columnIndex: columnIndex,
		predicate:   predicate,
	}

	// Create a filter function that checks the column
	filterFn := func(key MultiColumnValue) bool {
		// Make sure the key has enough columns
		if columnIndex >= len(key) {
			return false
		}

		// Get the column value and apply the predicate
		colValue := key[columnIndex]
		if colValue == nil {
			return false
		}

		// Use the raw value for the predicate
		return predicate(colValue)
	}

	// Create the base filter iterator
	iter.FilterIterator = NewFilterIterator(tree, mutex, filterFn)

	return iter
}
