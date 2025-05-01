// Package common provides shared types and utilities
package common

import (
	"sync"

	"github.com/semihalev/stoolap/internal/storage"
)

// Global sync.Pool for map[string]storage.ColumnValue to reduce allocations
var (
	SmallColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 8) // For small number of columns
		},
	}

	MediumColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 32) // For medium number of columns
		},
	}

	LargeColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 64) // For large number of columns
		},
	}
)

// GetColumnValueMapPool returns the appropriate map pool based on column count
func GetColumnValueMapPool(columnCount int) *sync.Pool {
	if columnCount <= 8 {
		return SmallColumnValueMapPool
	} else if columnCount <= 32 {
		return MediumColumnValueMapPool
	}
	return LargeColumnValueMapPool
}

// PutColumnValueMap returns a map to the appropriate pool
func PutColumnValueMap(m map[string]storage.ColumnValue, columnCount int) {
	// Clear the map before returning it to the pool
	clear(m)

	// Return to the appropriate pool based on size
	pool := GetColumnValueMapPool(columnCount)
	pool.Put(m)
}

// GetColumnValueMap gets a map from the appropriate pool based on expected size
func GetColumnValueMap(columnCount int) map[string]storage.ColumnValue {
	pool := GetColumnValueMapPool(columnCount)
	m := pool.Get().(map[string]storage.ColumnValue)

	// Map should already be cleared when returned to pool, but clear it just in case
	clear(m)

	return m
}
