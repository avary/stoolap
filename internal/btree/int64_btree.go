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
package btree

import (
	"iter"
	"sort"
)

// Constants for BTree configuration
const (
	// Using a medium order for good balance between memory usage and performance
	bTreeDegree    = 32               // Degree of the tree (max children per node)
	bTreeMaxKeys   = bTreeDegree - 1  // Maximum number of keys per node
	bTreeMinKeys   = bTreeMaxKeys / 2 // Minimum number of keys per node (except root)
	bTreeArraySize = bTreeMaxKeys + 1 // Size of arrays to allocate (with room for temporary overflow)
)

// Int64BTree is a highly optimized B-tree for int64 keys
// Key design points:
// 1. Fixed size arrays for keys/values/children to avoid resizing
// 2. Cache-friendly memory layout
// 3. Binary search for fast key lookup
// 4. Block transfer for bulk operations
type Int64BTree[V any] struct {
	root *bTreeNode[V] // Root node
	size int           // Number of keys in the tree
}

// bTreeNode represents a node in the BTree
type bTreeNode[V any] struct {
	keys     [bTreeArraySize]int64             // Keys stored in this node (sorted)
	values   [bTreeArraySize]V                 // Values corresponding to keys
	children [bTreeArraySize + 1]*bTreeNode[V] // Child pointers
	keyCount int                               // Number of keys actually stored
	isLeaf   bool                              // Whether this is a leaf node
}

// NewInt64BTree creates a new BTree optimized for int64 keys
func NewInt64BTree[V any]() *Int64BTree[V] {
	// Create a new tree with an empty root
	tree := &Int64BTree[V]{
		root: &bTreeNode[V]{
			keyCount: 0,
			isLeaf:   true,
		},
		size: 0,
	}
	return tree
}

// Size returns the number of key-value pairs in the tree
func (t *Int64BTree[V]) Size() int {
	return t.size
}

// Search looks up a key in the tree
// Returns the value and true if found, or zero value and false if not found
func (t *Int64BTree[V]) Search(key int64) (V, bool) {
	return t.searchNode(t.root, key)
}

// searchNode recursively searches for a key in a node and its children
func (t *Int64BTree[V]) searchNode(node *bTreeNode[V], key int64) (V, bool) {
	// Find the first key greater than or equal to the target
	i := t.binarySearch(node, key)

	// Check if we found an exact match
	if i < node.keyCount && node.keys[i] == key {
		return node.values[i], true
	}

	// If we're at a leaf, the key isn't in the tree
	if node.isLeaf {
		var zero V
		return zero, false
	}

	// Otherwise, recursively search the appropriate child
	return t.searchNode(node.children[i], key)
}

// binarySearch finds the position where a key should be in a node
// Returns the index of the first key greater than or equal to the target
func (t *Int64BTree[V]) binarySearch(node *bTreeNode[V], key int64) int {
	// Fast path for small nodes - linear search
	if node.keyCount <= 8 {
		for i := 0; i < node.keyCount; i++ {
			if node.keys[i] >= key {
				return i
			}
		}
		return node.keyCount
	}

	// Binary search for the first key >= target
	left, right := 0, node.keyCount-1
	for left <= right {
		mid := left + (right-left)/2
		if node.keys[mid] < key {
			left = mid + 1
		} else if node.keys[mid] > key {
			right = mid - 1
		} else {
			return mid // Exact match
		}
	}
	return left // First key >= target (or node.keyCount if all keys < target)
}

// Insert adds or updates a key-value pair in the tree
// Returns true if a new key was inserted, false if an existing key was updated
func (t *Int64BTree[V]) Insert(key int64, value V) {
	// Special case for root overflow
	if t.root.keyCount == bTreeMaxKeys {
		// Create a new root
		newRoot := &bTreeNode[V]{
			keyCount: 0,
			isLeaf:   false,
		}
		// Make the old root its first child
		newRoot.children[0] = t.root
		// Split the old root and elevate a key to the new root
		t.splitChild(newRoot, 0)
		// Set the new root
		t.root = newRoot
		// Continue with insertion at the new root
	}

	// Perform the insertion in the non-full root
	wasInserted := t.insertNonFull(t.root, key, value)
	if wasInserted {
		t.size++
	}
}

// insertNonFull inserts a key-value pair into a non-full node
func (t *Int64BTree[V]) insertNonFull(node *bTreeNode[V], key int64, value V) bool {
	// Find the insertion position
	i := t.binarySearch(node, key)

	// If the key exists, update its value and return false
	if i < node.keyCount && node.keys[i] == key {
		node.values[i] = value
		return false
	}

	// If this is a leaf node, insert directly
	if node.isLeaf {
		// Make room for the new key by shifting elements right
		for j := node.keyCount; j > i; j-- {
			node.keys[j] = node.keys[j-1]
			node.values[j] = node.values[j-1]
		}
		// Insert the new key and value
		node.keys[i] = key
		node.values[i] = value
		node.keyCount++
		return true
	}

	// This is an internal node, check if the child is full
	childIndex := i
	childNode := node.children[childIndex]
	if childNode.keyCount == bTreeMaxKeys {
		// Split the child
		t.splitChild(node, childIndex)
		// After the split, the middle key is at position i in the parent
		// Decide which child to follow after the split
		if key == node.keys[i] {
			// Update the promoted key directly
			node.values[i] = value
			return false
		} else if key > node.keys[i] {
			childIndex++
		}
		childNode = node.children[childIndex]
	}

	// Recursively insert into the child
	return t.insertNonFull(childNode, key, value)
}

// splitChild splits a full child of a node
// parent: the parent node, childIndex: index of the child to split
func (t *Int64BTree[V]) splitChild(parent *bTreeNode[V], childIndex int) {
	// The child to split
	child := parent.children[childIndex]

	// Create a new node for the right half of the child
	newChild := &bTreeNode[V]{
		keyCount: bTreeMinKeys,
		isLeaf:   child.isLeaf,
	}

	// Copy the right half of keys and values to the new child
	for j := 0; j < bTreeMinKeys; j++ {
		newChild.keys[j] = child.keys[j+bTreeMinKeys+1]
		newChild.values[j] = child.values[j+bTreeMinKeys+1]
	}

	// If the child is not a leaf, also copy the right half of child pointers
	if !child.isLeaf {
		for j := 0; j <= bTreeMinKeys; j++ {
			newChild.children[j] = child.children[j+bTreeMinKeys+1]
		}
	}

	// Update the key count of the original child (left half)
	child.keyCount = bTreeMinKeys

	// Make room in the parent for the new child pointer
	for j := parent.keyCount; j > childIndex; j-- {
		parent.children[j+1] = parent.children[j]
	}
	parent.children[childIndex+1] = newChild

	// Make room in the parent for the median key
	for j := parent.keyCount - 1; j >= childIndex; j-- {
		parent.keys[j+1] = parent.keys[j]
		parent.values[j+1] = parent.values[j]
	}

	// Copy the median key to the parent
	parent.keys[childIndex] = child.keys[bTreeMinKeys]
	parent.values[childIndex] = child.values[bTreeMinKeys]
	parent.keyCount++
}

// Delete removes a key-value pair from the tree
// Returns true if the key was found and deleted, false otherwise
func (t *Int64BTree[V]) Delete(key int64) bool {
	deleted := t.deleteKey(t.root, key)
	if deleted {
		t.size--
		// If the root is empty and has a child, make the child the new root
		if t.root.keyCount == 0 && !t.root.isLeaf {
			t.root = t.root.children[0]
		}
	}
	return deleted
}

// deleteKey removes a key from a node, recursively if necessary
func (t *Int64BTree[V]) deleteKey(node *bTreeNode[V], key int64) bool {
	// Find the position of the key or where it would be
	i := t.binarySearch(node, key)

	// Case 1: Key found in this node
	if i < node.keyCount && node.keys[i] == key {
		if node.isLeaf {
			// Case 1a: Key is in a leaf node - simple removal
			t.removeFromLeaf(node, i)
		} else {
			// Case 1b: Key is in an internal node
			t.removeFromNonLeaf(node, i)
		}
		return true
	}

	// Case 2: Key not found in this node
	if node.isLeaf {
		// Case 2a: This is a leaf node, so the key is not in the tree
		return false
	}

	// Case 2b: The key may be in a child
	// Check if the child at index i has at least bTreeMinKeys+1 keys
	childIndex := i
	child := node.children[childIndex]
	if child.keyCount <= bTreeMinKeys {
		// Child has minimum keys, need to ensure it has at least bTreeMinKeys+1
		t.fillChild(node, childIndex)
	}

	// If the last child was merged, need to go to the previous child
	if childIndex > node.keyCount {
		childIndex = node.keyCount
	}

	// Recursively delete from the child
	return t.deleteKey(node.children[childIndex], key)
}

// removeFromLeaf removes a key from a leaf node
func (t *Int64BTree[V]) removeFromLeaf(node *bTreeNode[V], index int) {
	// Shift all keys and values after index one position left
	for i := index + 1; i < node.keyCount; i++ {
		node.keys[i-1] = node.keys[i]
		node.values[i-1] = node.values[i]
	}
	node.keyCount--
}

// removeFromNonLeaf removes a key from an internal node
func (t *Int64BTree[V]) removeFromNonLeaf(node *bTreeNode[V], index int) {
	key := node.keys[index]

	// Case 1: If the left child has at least bTreeMinKeys+1 keys,
	// find the predecessor and replace the key with it
	leftChild := node.children[index]
	if leftChild.keyCount > bTreeMinKeys {
		predecessor := t.getLastKey(leftChild)
		node.keys[index] = predecessor.key
		node.values[index] = predecessor.value
		t.deleteKey(leftChild, predecessor.key)
		return
	}

	// Case 2: If the right child has at least bTreeMinKeys+1 keys,
	// find the successor and replace the key with it
	rightChild := node.children[index+1]
	if rightChild.keyCount > bTreeMinKeys {
		successor := t.getFirstKey(rightChild)
		node.keys[index] = successor.key
		node.values[index] = successor.value
		t.deleteKey(rightChild, successor.key)
		return
	}

	// Case 3: Both left and right children have exactly bTreeMinKeys keys
	// Merge the right child into the left child, including the key from the node
	t.mergeChildren(node, index)
	t.deleteKey(leftChild, key)
}

// getLastKey returns the last key-value pair in the subtree rooted at node
type keyValuePair[V any] struct {
	key   int64
	value V
}

func (t *Int64BTree[V]) getLastKey(node *bTreeNode[V]) keyValuePair[V] {
	current := node
	for !current.isLeaf {
		current = current.children[current.keyCount]
	}
	return keyValuePair[V]{
		key:   current.keys[current.keyCount-1],
		value: current.values[current.keyCount-1],
	}
}

// getFirstKey returns the first key-value pair in the subtree rooted at node
func (t *Int64BTree[V]) getFirstKey(node *bTreeNode[V]) keyValuePair[V] {
	current := node
	for !current.isLeaf {
		current = current.children[0]
	}
	return keyValuePair[V]{
		key:   current.keys[0],
		value: current.values[0],
	}
}

// fillChild ensures that a child node has at least bTreeMinKeys+1 keys
func (t *Int64BTree[V]) fillChild(node *bTreeNode[V], index int) {
	// Case 1: Borrow from left sibling if it has extra keys
	if index > 0 && node.children[index-1].keyCount > bTreeMinKeys {
		t.borrowFromPrev(node, index)
		return
	}

	// Case 2: Borrow from right sibling if it has extra keys
	if index < node.keyCount && node.children[index+1].keyCount > bTreeMinKeys {
		t.borrowFromNext(node, index)
		return
	}

	// Case 3: Merge child with a sibling
	if index < node.keyCount {
		// Merge with right sibling
		t.mergeChildren(node, index)
	} else {
		// Merge with left sibling (index > 0 is guaranteed here)
		t.mergeChildren(node, index-1)
	}
}

// borrowFromPrev borrows a key from the left sibling
func (t *Int64BTree[V]) borrowFromPrev(node *bTreeNode[V], index int) {
	child := node.children[index]
	leftSibling := node.children[index-1]

	// Make room for the new key at the beginning of the child
	for i := child.keyCount - 1; i >= 0; i-- {
		child.keys[i+1] = child.keys[i]
		child.values[i+1] = child.values[i]
	}

	// If the child is not a leaf, also move child pointers
	if !child.isLeaf {
		for i := child.keyCount; i >= 0; i-- {
			child.children[i+1] = child.children[i]
		}
	}

	// Move a key from parent to the child
	child.keys[0] = node.keys[index-1]
	child.values[0] = node.values[index-1]

	// Move the last key from the left sibling to the parent
	node.keys[index-1] = leftSibling.keys[leftSibling.keyCount-1]
	node.values[index-1] = leftSibling.values[leftSibling.keyCount-1]

	// If the child is not a leaf, move the last child pointer of the left sibling
	if !child.isLeaf {
		child.children[0] = leftSibling.children[leftSibling.keyCount]
	}

	// Update key counts
	child.keyCount++
	leftSibling.keyCount--
}

// borrowFromNext borrows a key from the right sibling
func (t *Int64BTree[V]) borrowFromNext(node *bTreeNode[V], index int) {
	child := node.children[index]
	rightSibling := node.children[index+1]

	// Move a key from parent to the end of the child
	child.keys[child.keyCount] = node.keys[index]
	child.values[child.keyCount] = node.values[index]

	// If the child is not a leaf, move the first child pointer of the right sibling
	if !child.isLeaf {
		child.children[child.keyCount+1] = rightSibling.children[0]
	}

	// Move the first key from the right sibling to the parent
	node.keys[index] = rightSibling.keys[0]
	node.values[index] = rightSibling.values[0]

	// Shift all keys and values in the right sibling one position left
	for i := 1; i < rightSibling.keyCount; i++ {
		rightSibling.keys[i-1] = rightSibling.keys[i]
		rightSibling.values[i-1] = rightSibling.values[i]
	}

	// If the right sibling is not a leaf, also shift child pointers
	if !rightSibling.isLeaf {
		for i := 1; i <= rightSibling.keyCount; i++ {
			rightSibling.children[i-1] = rightSibling.children[i]
		}
	}

	// Update key counts
	child.keyCount++
	rightSibling.keyCount--
}

// mergeChildren merges two child nodes, moving a key from the parent
func (t *Int64BTree[V]) mergeChildren(node *bTreeNode[V], index int) {
	leftChild := node.children[index]
	rightChild := node.children[index+1]

	// Move the key from the parent to the left child
	leftChild.keys[leftChild.keyCount] = node.keys[index]
	leftChild.values[leftChild.keyCount] = node.values[index]

	// Copy all keys and values from the right child to the left child
	for i := 0; i < rightChild.keyCount; i++ {
		leftChild.keys[leftChild.keyCount+1+i] = rightChild.keys[i]
		leftChild.values[leftChild.keyCount+1+i] = rightChild.values[i]
	}

	// If the children are not leaves, copy the child pointers as well
	if !leftChild.isLeaf {
		for i := 0; i <= rightChild.keyCount; i++ {
			leftChild.children[leftChild.keyCount+1+i] = rightChild.children[i]
		}
	}

	// Update the key count of the left child
	leftChild.keyCount += rightChild.keyCount + 1

	// Shift keys and values in the parent to close the gap
	for i := index + 1; i < node.keyCount; i++ {
		node.keys[i-1] = node.keys[i]
		node.values[i-1] = node.values[i]
	}

	// Shift child pointers in the parent to close the gap
	for i := index + 2; i <= node.keyCount; i++ {
		node.children[i-1] = node.children[i]
	}

	// Decrease the key count of the parent
	node.keyCount--
}

// RangeSearch finds all values with keys in range [start, end] inclusive
func (t *Int64BTree[V]) RangeSearch(start, end int64) []V {
	if t.root == nil || start > end {
		return []V{}
	}

	// Use a pre-allocated result slice with generous capacity estimation
	// For large ranges with sparse data, we need to be conservative
	resultCapacity := 128
	// For very sparse data like our test case with 1M increments
	if (end - start) > 1000000 {
		resultCapacity = int((end-start)/1000000) + 10
	}
	if t.size < resultCapacity {
		resultCapacity = t.size
	}

	results := make([]V, 0, resultCapacity)

	// Use optimized traversal that only visits nodes that could contain keys in the range
	t.rangeSearchNode(t.root, start, end, &results)

	return results
}

// rangeSearchNode performs an optimized range search by only traversing nodes
// that could contain keys in the range [start, end]
func (t *Int64BTree[V]) rangeSearchNode(node *bTreeNode[V], start, end int64, results *[]V) {
	if node == nil {
		return
	}

	// For leaf nodes, directly find and add keys in the range
	if node.isLeaf {
		for i := 0; i < node.keyCount; i++ {
			if node.keys[i] >= start && node.keys[i] <= end {
				*results = append(*results, node.values[i])
			}
		}
		return
	}

	// First find position where keys could be >= start
	startPos := 0
	for startPos < node.keyCount && node.keys[startPos] < start {
		startPos++
	}

	// Always check the child that might contain the start value
	// For a B-tree, if we're at position i, values < keys[i] are in children[i]
	t.rangeSearchNode(node.children[startPos], start, end, results)

	// Process current node's keys and their right children
	for i := startPos; i < node.keyCount; i++ {
		// If this key is in range, add it
		if node.keys[i] >= start && node.keys[i] <= end {
			*results = append(*results, node.values[i])
		}

		// If the key exceeds our end range, we're done
		if node.keys[i] > end {
			break
		}

		// Check the child to the right of this key
		t.rangeSearchNode(node.children[i+1], start, end, results)
	}
}

// ForEach visits all keys in the tree in order
func (t *Int64BTree[V]) ForEach(callback func(key int64, value V) bool) {
	if t.root == nil {
		return
	}
	t.forEachNode(t.root, callback)
}

// forEachNode visits all keys in a subtree in order
func (t *Int64BTree[V]) forEachNode(node *bTreeNode[V], callback func(key int64, value V) bool) bool {
	if node == nil {
		return true
	}

	// For each key in this node:
	for i := 0; i < node.keyCount; i++ {
		// Visit the left child first
		if !node.isLeaf {
			if !t.forEachNode(node.children[i], callback) {
				return false
			}
		}

		// Visit this key
		if !callback(node.keys[i], node.values[i]) {
			return false
		}
	}

	// Visit the rightmost child if there is one
	if !node.isLeaf {
		if !t.forEachNode(node.children[node.keyCount], callback) {
			return false
		}
	}

	return true
}

// GetAll returns all values in the tree
func (t *Int64BTree[V]) GetAll() iter.Seq2[int64, V] {
	return t.ForEach
}

// BatchInsert efficiently inserts multiple key-value pairs
func (t *Int64BTree[V]) BatchInsert(keys []int64, values []V) {
	if len(keys) != len(values) {
		panic("keys and values must have the same length")
	}

	if len(keys) == 0 {
		return
	}

	// Create pairs and sort them for efficient insertion
	type pair struct {
		key   int64
		value V
	}

	pairs := make([]pair, len(keys))
	for i := range keys {
		pairs[i] = pair{key: keys[i], value: values[i]}
	}

	// Sort by key
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})

	// Insert in batches
	batchSize := 1000
	for i := 0; i < len(pairs); i += batchSize {
		end := i + batchSize
		if end > len(pairs) {
			end = len(pairs)
		}

		for j := i; j < end; j++ {
			t.Insert(pairs[j].key, pairs[j].value)
		}
	}
}
