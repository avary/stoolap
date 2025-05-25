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
// Package btree provides optimized B-tree implementations
package btree

import (
	"fmt"
)

// Comparer defines the interface for objects that can be used as keys in a BTree
type Comparer[K any] interface {
	// Compare compares this key with another key
	// Returns negative if this < other, 0 if equal, positive if this > other
	Compare(other K) int
}

// BTree is a generic B-tree implementation for any key type that implements Comparer
type BTree[K Comparer[K], V any] struct {
	root *btreeNode[K, V] // Root node
	size int              // Number of keys in the tree
}

// btreeNode represents a node in the BTree
type btreeNode[K Comparer[K], V any] struct {
	keys     []K                // Keys stored in this node (sorted)
	values   []V                // Values corresponding to keys
	children []*btreeNode[K, V] // Child pointers
	isLeaf   bool               // Whether this is a leaf node
}

// NewBTree creates a new BTree for keys of type K that implement Comparer
func NewBTree[K Comparer[K], V any]() *BTree[K, V] {
	// Create a new tree with an empty root
	return &BTree[K, V]{
		root: &btreeNode[K, V]{
			keys:     make([]K, 0),
			values:   make([]V, 0),
			children: make([]*btreeNode[K, V], 0),
			isLeaf:   true,
		},
		size: 0,
	}
}

// Size returns the number of key-value pairs in the tree
func (t *BTree[K, V]) Size() int {
	return t.size
}

// Search looks up a key in the tree
// Returns the value and true if found, or zero value and false if not found
func (t *BTree[K, V]) Search(key K) (V, bool) {
	if t.root == nil {
		var zero V
		return zero, false
	}
	return t.searchNode(t.root, key)
}

// searchNode recursively searches for a key in a node and its children
func (t *BTree[K, V]) searchNode(node *btreeNode[K, V], key K) (V, bool) {
	// Binary search to find the key or the position it should be
	i := t.findPosition(node, key)

	// Check if we found an exact match
	if i < len(node.keys) && node.keys[i].Compare(key) == 0 {
		return node.values[i], true
	}

	// If this is a leaf node, the key is not in the tree
	if node.isLeaf {
		var zero V
		return zero, false
	}

	// Otherwise, search in the appropriate child
	return t.searchNode(node.children[i], key)
}

// findPosition finds the position for a key in a node using binary search
// Returns the index where the key should be inserted
func (t *BTree[K, V]) findPosition(node *btreeNode[K, V], key K) int {
	// Binary search
	left, right := 0, len(node.keys)-1
	for left <= right {
		mid := left + (right-left)/2
		cmp := key.Compare(node.keys[mid])
		if cmp < 0 {
			right = mid - 1
		} else if cmp > 0 {
			left = mid + 1
		} else {
			return mid // Found an exact match
		}
	}
	return left // Position where the key should be
}

// Insert adds or updates a key-value pair in the tree
func (t *BTree[K, V]) Insert(key K, value V) {
	// Handle empty tree
	if t.root == nil {
		t.root = &btreeNode[K, V]{
			keys:     []K{key},
			values:   []V{value},
			children: make([]*btreeNode[K, V], 0),
			isLeaf:   true,
		}
		t.size++
		return
	}

	// If the root is full, split it
	if len(t.root.keys) >= 32 { // Max node size
		// Create a new root
		newRoot := &btreeNode[K, V]{
			keys:     make([]K, 0),
			values:   make([]V, 0),
			children: []*btreeNode[K, V]{t.root},
			isLeaf:   false,
		}
		t.root = newRoot
		t.splitChild(newRoot, 0)
	}

	// Insert into the non-full root
	wasInserted := t.insertNonFull(t.root, key, value)
	if wasInserted {
		t.size++
	}
}

// insertNonFull inserts a key-value pair into a non-full node
func (t *BTree[K, V]) insertNonFull(node *btreeNode[K, V], key K, value V) bool {
	// Find position where the key should go
	i := t.findPosition(node, key)

	// If we found an exact match, update the value
	if i < len(node.keys) && node.keys[i].Compare(key) == 0 {
		node.values[i] = value
		return false
	}

	// If this is a leaf node, insert directly
	if node.isLeaf {
		// Insert at position i
		node.keys = append(node.keys, key) // Append to make room
		node.values = append(node.values, value)
		// Shift elements to insert at position i
		if i < len(node.keys)-1 {
			copy(node.keys[i+1:], node.keys[i:len(node.keys)-1])
			copy(node.values[i+1:], node.values[i:len(node.values)-1])
			node.keys[i] = key
			node.values[i] = value
		}
		return true
	}

	// If the child is full, split it
	if len(node.children[i].keys) >= 32 {
		t.splitChild(node, i)
		// After splitting, the median key is at position i in the parent
		// Need to determine which child to go to now
		if node.keys[i].Compare(key) < 0 {
			i++
		}
	}

	// Recursively insert into the appropriate child
	return t.insertNonFull(node.children[i], key, value)
}

// splitChild splits a full child of a node
func (t *BTree[K, V]) splitChild(parent *btreeNode[K, V], childIndex int) {
	child := parent.children[childIndex]
	midpoint := len(child.keys) / 2

	// Create a new right node
	rightNode := &btreeNode[K, V]{
		keys:   append([]K{}, child.keys[midpoint+1:]...),
		values: append([]V{}, child.values[midpoint+1:]...),
		isLeaf: child.isLeaf,
	}

	// If not a leaf, distribute children too
	if !child.isLeaf {
		rightNode.children = append([]*btreeNode[K, V]{}, child.children[midpoint+1:]...)
	}

	// Move the median key up to the parent
	medianKey := child.keys[midpoint]
	medianValue := child.values[midpoint]

	// Truncate the child to only include left half
	child.keys = child.keys[:midpoint]
	child.values = child.values[:midpoint]
	if !child.isLeaf {
		child.children = child.children[:midpoint+1]
	}

	// Insert the new child into the parent
	// First, make room for the new key and child
	parent.keys = append(parent.keys, medianKey)
	parent.values = append(parent.values, medianValue)
	parent.children = append(parent.children, nil)

	// Shift elements to insert at the right position
	if childIndex < len(parent.keys)-1 {
		copy(parent.keys[childIndex+1:], parent.keys[childIndex:len(parent.keys)-1])
		copy(parent.values[childIndex+1:], parent.values[childIndex:len(parent.values)-1])
		copy(parent.children[childIndex+2:], parent.children[childIndex+1:len(parent.children)-1])
		parent.keys[childIndex] = medianKey
		parent.values[childIndex] = medianValue
	}
	parent.children[childIndex+1] = rightNode
}

// Delete removes a key-value pair from the tree
func (t *BTree[K, V]) Delete(key K) bool {
	if t.root == nil {
		return false
	}

	deleted := t.deleteKey(t.root, key)
	if deleted {
		t.size--
		// If the root is now empty, make its only child the new root
		if len(t.root.keys) == 0 && !t.root.isLeaf {
			t.root = t.root.children[0]
		}
	}
	return deleted
}

// deleteKey removes a key from a node or its children
func (t *BTree[K, V]) deleteKey(node *btreeNode[K, V], key K) bool {
	// Find the position where the key should be
	i := t.findPosition(node, key)

	// Check if the key is in this node
	keyFound := i < len(node.keys) && node.keys[i].Compare(key) == 0

	if keyFound {
		// If we're at a leaf, simply remove the key
		if node.isLeaf {
			// Remove the key and value by shifting elements
			copy(node.keys[i:], node.keys[i+1:])
			copy(node.values[i:], node.values[i+1:])
			node.keys = node.keys[:len(node.keys)-1]
			node.values = node.values[:len(node.values)-1]
			return true
		}

		// If we're at an internal node, replace with predecessor or successor
		if len(node.children[i].keys) > 15 { // Left child has extra keys
			// Find the predecessor - the rightmost key in the left subtree
			pred, predVal := t.findRightmostInSubtree(node.children[i])
			node.keys[i] = pred
			node.values[i] = predVal
			return t.deleteKey(node.children[i], pred)
		} else if len(node.children[i+1].keys) > 15 { // Right child has extra keys
			// Find the successor - the leftmost key in the right subtree
			succ, succVal := t.findLeftmostInSubtree(node.children[i+1])
			node.keys[i] = succ
			node.values[i] = succVal
			return t.deleteKey(node.children[i+1], succ)
		} else {
			// Both children have minimum keys, merge them
			mergeKey := node.keys[i]
			t.mergeNodes(node, i)
			// Now the key is in the merged node, delete it from there
			return t.deleteKey(node.children[i], mergeKey)
		}
	} else {
		// The key must be in a child if it exists
		if node.isLeaf {
			return false // Key not found
		}

		// Check if the child has enough keys
		childIndex := i
		if len(node.children[childIndex].keys) <= 15 {
			t.ensureChildHasEnoughKeys(node, childIndex)
		}

		// Recursive call to delete from the appropriate child
		return t.deleteKey(node.children[childIndex], key)
	}
}

// findLeftmostInSubtree finds the leftmost key in a subtree
func (t *BTree[K, V]) findLeftmostInSubtree(node *btreeNode[K, V]) (K, V) {
	if node.isLeaf {
		return node.keys[0], node.values[0]
	}
	return t.findLeftmostInSubtree(node.children[0])
}

// findRightmostInSubtree finds the rightmost key in a subtree
func (t *BTree[K, V]) findRightmostInSubtree(node *btreeNode[K, V]) (K, V) {
	if node.isLeaf {
		lastIdx := len(node.keys) - 1
		return node.keys[lastIdx], node.values[lastIdx]
	}
	return t.findRightmostInSubtree(node.children[len(node.children)-1])
}

// ensureChildHasEnoughKeys ensures a child node has enough keys (at least 16 in a standard B-tree)
func (t *BTree[K, V]) ensureChildHasEnoughKeys(node *btreeNode[K, V], childIndex int) {
	// Try to borrow from left sibling
	if childIndex > 0 && len(node.children[childIndex-1].keys) > 15 {
		t.borrowFromLeftSibling(node, childIndex)
		return
	}

	// Try to borrow from right sibling
	if childIndex < len(node.children)-1 && len(node.children[childIndex+1].keys) > 15 {
		t.borrowFromRightSibling(node, childIndex)
		return
	}

	// Merge with a sibling
	if childIndex > 0 {
		// Merge with left sibling
		t.mergeNodes(node, childIndex-1)
	} else {
		// Merge with right sibling
		t.mergeNodes(node, childIndex)
	}
}

// borrowFromLeftSibling borrows a key from the left sibling
func (t *BTree[K, V]) borrowFromLeftSibling(node *btreeNode[K, V], childIndex int) {
	child := node.children[childIndex]
	leftSibling := node.children[childIndex-1]

	// Move key from parent to child
	child.keys = append([]K{node.keys[childIndex-1]}, child.keys...)
	child.values = append([]V{node.values[childIndex-1]}, child.values...)

	// Move key from left sibling to parent
	lastKeyIdx := len(leftSibling.keys) - 1
	node.keys[childIndex-1] = leftSibling.keys[lastKeyIdx]
	node.values[childIndex-1] = leftSibling.values[lastKeyIdx]

	// If not leaf nodes, move child pointers too
	if !child.isLeaf {
		lastChildIdx := len(leftSibling.children) - 1
		child.children = append([]*btreeNode[K, V]{leftSibling.children[lastChildIdx]}, child.children...)
		leftSibling.children = leftSibling.children[:lastChildIdx]
	}

	// Remove the borrowed key from left sibling
	leftSibling.keys = leftSibling.keys[:lastKeyIdx]
	leftSibling.values = leftSibling.values[:lastKeyIdx]
}

// borrowFromRightSibling borrows a key from the right sibling
func (t *BTree[K, V]) borrowFromRightSibling(node *btreeNode[K, V], childIndex int) {
	child := node.children[childIndex]
	rightSibling := node.children[childIndex+1]

	// Move key from parent to child
	child.keys = append(child.keys, node.keys[childIndex])
	child.values = append(child.values, node.values[childIndex])

	// Move key from right sibling to parent
	node.keys[childIndex] = rightSibling.keys[0]
	node.values[childIndex] = rightSibling.values[0]

	// If not leaf nodes, move child pointers too
	if !child.isLeaf {
		child.children = append(child.children, rightSibling.children[0])
		rightSibling.children = rightSibling.children[1:]
	}

	// Remove the borrowed key from right sibling
	rightSibling.keys = rightSibling.keys[1:]
	rightSibling.values = rightSibling.values[1:]
}

// mergeNodes merges a child with its sibling
func (t *BTree[K, V]) mergeNodes(node *btreeNode[K, V], keyIndex int) {
	leftChild := node.children[keyIndex]
	rightChild := node.children[keyIndex+1]

	// Add key from parent to the left child
	leftChild.keys = append(leftChild.keys, node.keys[keyIndex])
	leftChild.values = append(leftChild.values, node.values[keyIndex])

	// Add all keys and values from right child
	leftChild.keys = append(leftChild.keys, rightChild.keys...)
	leftChild.values = append(leftChild.values, rightChild.values...)

	// If not leaf nodes, move child pointers too
	if !leftChild.isLeaf {
		leftChild.children = append(leftChild.children, rightChild.children...)
	}

	// Remove key and child pointers from parent
	copy(node.keys[keyIndex:], node.keys[keyIndex+1:])
	copy(node.values[keyIndex:], node.values[keyIndex+1:])
	copy(node.children[keyIndex+1:], node.children[keyIndex+2:])
	node.keys = node.keys[:len(node.keys)-1]
	node.values = node.values[:len(node.values)-1]
	node.children = node.children[:len(node.children)-1]
}

// Iterator provides a way to iterate through all key-value pairs in the BTree
type Iterator[K Comparer[K], V any] struct {
	// Stack of nodes and indices for traversal
	stack [][2]interface{} // Stores [*btreeNode[K, V], int] pairs

	// Current key and value
	currKey   K
	currValue V

	// Valid flag
	valid bool
}

// Iterate creates an iterator for the BTree
func (t *BTree[K, V]) Iterate() *Iterator[K, V] {
	iter := &Iterator[K, V]{
		stack: make([][2]interface{}, 0, 8),
		valid: false,
	}

	// Initialize the iterator by positioning at the leftmost leaf
	if t.root != nil {
		iter.pushLeftEdge(t.root)
	}

	// Position at the first element
	iter.Next()

	return iter
}

// pushLeftEdge adds all nodes along the left edge of the subtree to the stack
func (iter *Iterator[K, V]) pushLeftEdge(node *btreeNode[K, V]) {
	for node != nil {
		// Add node to stack with index -1 (before first key)
		iter.stack = append(iter.stack, [2]interface{}{node, -1})
		if node.isLeaf {
			break
		}
		node = node.children[0]
	}
}

// Next advances the iterator to the next key-value pair
func (iter *Iterator[K, V]) Next() bool {
	if len(iter.stack) == 0 {
		iter.valid = false
		return false
	}

	// Get the top node and index from the stack
	top := len(iter.stack) - 1
	node := iter.stack[top][0].(*btreeNode[K, V])
	index := iter.stack[top][1].(int) + 1 // Move to next index

	// Update the top of the stack
	iter.stack[top][1] = index

	// Check if we've processed all keys in this node
	if index >= len(node.keys) {
		// Pop this node from the stack
		iter.stack = iter.stack[:top]
		// Try to move to the next element
		return iter.Next()
	}

	// Set the current key and value
	iter.currKey = node.keys[index]
	iter.currValue = node.values[index]
	iter.valid = true

	// If not a leaf, push the child node to the right of this key
	if !node.isLeaf && len(node.children) > index+1 {
		iter.pushLeftEdge(node.children[index+1])
	}

	return true
}

// Get returns the current key and value
func (iter *Iterator[K, V]) Get() (K, V) {
	return iter.currKey, iter.currValue
}

// Valid returns whether the iterator is positioned at a valid element
func (iter *Iterator[K, V]) Valid() bool {
	return iter.valid
}

// SeekGE positions the iterator at the first key greater than or equal to the target
func (t *BTree[K, V]) SeekGE(target K) *Iterator[K, V] {
	iter := &Iterator[K, V]{
		stack: make([][2]interface{}, 0, 8),
		valid: false,
	}

	if t.root == nil {
		return iter
	}

	// Search for the target key
	t.seekGENode(iter, t.root, target)

	// Position at the first valid element
	iter.Next()

	return iter
}

// seekGENode recursively searches for a key >= target
func (t *BTree[K, V]) seekGENode(iter *Iterator[K, V], node *btreeNode[K, V], target K) bool {
	// Binary search to find the position for this key
	i := t.findPosition(node, target)

	// Check if we found an exact match
	if i < len(node.keys) && node.keys[i].Compare(target) == 0 {
		// For exact matches, position at index i-1 so Next() will include this key
		iter.stack = append(iter.stack, [2]interface{}{node, i - 1})
		return false
	}

	// If this is a leaf, add it to the stack with the appropriate index
	if node.isLeaf {
		if i > 0 {
			// Position before the next key (or at the end)
			iter.stack = append(iter.stack, [2]interface{}{node, i - 1})
		} else {
			// Position at the beginning
			iter.stack = append(iter.stack, [2]interface{}{node, -1})
		}
		return false
	}

	// Recursively search in the appropriate child
	iter.stack = append(iter.stack, [2]interface{}{node, i - 1})
	return t.seekGENode(iter, node.children[i], target)
}

// ForEach visits all keys in the tree in order
func (t *BTree[K, V]) ForEach(callback func(key K, value V) bool) {
	if t.root == nil {
		return
	}
	t.forEachNode(t.root, callback)
}

// forEachNode visits all keys in a subtree in order
func (t *BTree[K, V]) forEachNode(node *btreeNode[K, V], callback func(key K, value V) bool) bool {
	if node == nil {
		return true
	}

	for i := 0; i < len(node.keys); i++ {
		// Visit left child first
		if !node.isLeaf && i < len(node.children) {
			if !t.forEachNode(node.children[i], callback) {
				return false
			}
		}

		// Visit key-value pair
		if !callback(node.keys[i], node.values[i]) {
			return false
		}
	}

	// Visit rightmost child
	if !node.isLeaf && len(node.children) > len(node.keys) {
		if !t.forEachNode(node.children[len(node.keys)], callback) {
			return false
		}
	}

	return true
}

// String returns a string representation of the tree (for debugging)
func (t *BTree[K, V]) String() string {
	if t.root == nil {
		return "Empty BTree"
	}

	result := fmt.Sprintf("BTree (size=%d)\n", t.size)
	return result + t.stringNode(t.root, 0)
}

// stringNode returns a string representation of a node and its subtree
func (t *BTree[K, V]) stringNode(node *btreeNode[K, V], level int) string {
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	result := indent + "Node("
	for i, key := range node.keys {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%v", key)
	}
	result += ")\n"

	if !node.isLeaf {
		for _, child := range node.children {
			result += t.stringNode(child, level+1)
		}
	}

	return result
}
