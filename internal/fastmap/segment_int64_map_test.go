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

package fastmap

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSegmentInt64MapBasicOperations(t *testing.T) {
	m := NewSegmentInt64Map[string](4, 100) // 16 segments, 100 initial capacity

	// Test Set and Get
	m.Set(1, "one")
	m.Set(2, "two")
	m.Set(3, "three")

	if val, ok := m.Get(1); !ok || val != "one" {
		t.Errorf("Expected Get(1) = \"one\", got %v, %v", val, ok)
	}

	if val, ok := m.Get(2); !ok || val != "two" {
		t.Errorf("Expected Get(2) = \"two\", got %v, %v", val, ok)
	}

	if val, ok := m.Get(3); !ok || val != "three" {
		t.Errorf("Expected Get(3) = \"three\", got %v, %v", val, ok)
	}

	// Test Has
	if !m.Has(1) {
		t.Errorf("Expected Has(1) = true")
	}

	if !m.Has(2) {
		t.Errorf("Expected Has(2) = true")
	}

	if !m.Has(3) {
		t.Errorf("Expected Has(3) = true")
	}

	if m.Has(4) {
		t.Errorf("Expected Has(4) = false")
	}

	// Test length
	if m.Len() != 3 {
		t.Errorf("Expected Len() = 3, got %d", m.Len())
	}

	// Test update
	m.Set(1, "ONE")
	if val, ok := m.Get(1); !ok || val != "ONE" {
		t.Errorf("Expected Get(1) = \"ONE\" after update, got %v, %v", val, ok)
	}

	// Test PutIfNotExists
	if val, ok := m.PutIfNotExists(1, "one again"); !ok && val != "ONE" {
		t.Errorf("Expected PutIfNotExists to not insert for existing key 1")
	}

	if val, ok := m.PutIfNotExists(4, "four"); !ok || val != "four" {
		t.Errorf("Expected PutIfNotExists to insert for new key 4")
	}

	// Test length after add
	if m.Len() != 4 {
		t.Errorf("Expected Len() = 4 after add, got %d", m.Len())
	}

	// Test deletion
	if !m.Del(1) {
		t.Errorf("Expected Del(1) = true")
	}

	if m.Has(1) {
		t.Errorf("Expected Has(1) = false after deletion")
	}

	// Test length after deletion
	if m.Len() != 3 {
		t.Errorf("Expected Len() = 3 after deletion, got %d", m.Len())
	}

	// Test deleting non-existent key
	if m.Del(999) {
		t.Errorf("Expected Del(999) = false for non-existent key")
	}

	// Test iteration
	keys := make(map[int64]bool)
	values := make(map[string]bool)

	m.ForEach(func(key int64, value string) bool {
		keys[key] = true
		values[value] = true
		return true
	})

	// Check keys and values
	for _, k := range []int64{2, 3, 4} {
		if !keys[k] {
			t.Errorf("Expected key %d in iteration", k)
		}
	}

	for _, v := range []string{"two", "three", "four"} {
		if !values[v] {
			t.Errorf("Expected value %s in iteration", v)
		}
	}

	// Test Clear
	m.Clear()
	if m.Len() != 0 {
		t.Errorf("Expected Len() = 0 after Clear(), got %d", m.Len())
	}

	if m.Has(2) || m.Has(3) || m.Has(4) {
		t.Errorf("Expected all keys to be gone after Clear()")
	}
}

func TestSegmentInt64MapConcurrency(t *testing.T) {
	// Use more segments than CPUs for better concurrency
	numCPU := runtime.NumCPU()
	var segPower uint8 = 4 // 16 segments
	if numCPU > 8 {
		segPower = 6 // 64 segments
	}

	m := NewSegmentInt64Map[int](segPower, 10000)
	const numOps = 100000
	const numGoroutines = 16

	var wg sync.WaitGroup
	opsPerGoroutine := numOps / numGoroutines

	// Counter for successful operations
	var setCount, getCount, delCount atomic.Int64

	// Run operations concurrently
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine works on a range of keys with some overlap
			keyOffset := id * (opsPerGoroutine / 2)

			for i := 0; i < opsPerGoroutine; i++ {
				key := int64(keyOffset + (i % (opsPerGoroutine / 2)))

				// Mix of operations
				op := i % 10

				switch {
				case op < 5: // 50% sets
					m.Set(key, id*1000+i)
					setCount.Add(1)
				case op < 9: // 40% gets
					if _, ok := m.Get(key); ok {
						getCount.Add(1)
					}
				default: // 10% deletes
					if m.Del(key) {
						delCount.Add(1)
					}
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify map state
	t.Logf("Concurrent operations completed: set=%d, successful get=%d, successful delete=%d",
		setCount.Load(), getCount.Load(), delCount.Load())
	t.Logf("Final map size: %d", m.Len())

	// Basic check that numbers are reasonable
	if m.Len() == 0 {
		t.Errorf("Expected non-empty map after operations")
	}

	if setCount.Load() == 0 || getCount.Load() == 0 || delCount.Load() == 0 {
		t.Errorf("Expected non-zero counts for all operations")
	}
}

func TestSegmentInt64MapConcurrentIteration(t *testing.T) {
	m := NewSegmentInt64Map[int](4, 1000) // 16 segments

	// Pre-populate map
	for i := 0; i < 1000; i++ {
		m.Set(int64(i), i)
	}

	// Test we can iterate while other operations are happening
	var wg sync.WaitGroup

	// Start modifiers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 1000; j++ {
				key := int64(j)

				// Mix of operations
				switch j % 3 {
				case 0:
					m.Set(key, j*10)
				case 1:
					_, _ = m.Get(key)
				case 2:
					if j%10 == 0 { // Delete some keys
						m.Del(key)
					}
				}
			}
		}()
	}

	// Start iterators
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Iterate multiple times while modifications happen
			for iter := 0; iter < 5; iter++ {
				count := 0
				m.ForEach(func(key int64, value int) bool {
					count++
					return true
				})

				// Don't assert exact count since it's changing
				if count == 0 {
					t.Errorf("Iterator found no entries")
				}
			}
		}()
	}

	wg.Wait()
}
