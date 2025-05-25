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
package fastmap

import (
	"math/rand"
	"testing"
)

// Generate different key patterns for comprehensive testing
func genRealisticKeys(n int) []int64 {
	keys := make([]int64, n)

	// First 40% sequential (like auto-incrementing IDs)
	for i := 0; i < n*4/10; i++ {
		keys[i] = int64(i + 1)
	}

	// Next 30% time-based (like timestamps with some randomness)
	const hourInNanos = int64(60 * 60 * 1000000000)
	startTime := int64(1609459200000000000) // 2021-01-01 in nanoseconds

	for i := n * 4 / 10; i < n*7/10; i++ {
		jitter := rand.Int63n(60 * 1000000000) // 60 seconds of jitter
		keys[i] = startTime + int64(i-(n*4/10))*hourInNanos + jitter
	}

	// Last 30% randomly distributed
	for i := n * 7 / 10; i < n; i++ {
		keys[i] = rand.Int63()
	}

	// Shuffle to simulate real access patterns
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

// Comprehensive benchmark comparing all map implementations
func BenchmarkSyncInt64Map(b *testing.B) {
	const keyCount = 100_000
	keys := genRealisticKeys(keyCount)

	benchMap := func(b *testing.B, readPct, writePct, deletePct int) {
		if readPct+writePct+deletePct != 100 {
			b.Fatalf("Percentages must sum to 100: %d + %d + %d", readPct, writePct, deletePct)
		}

		totalOps := 100
		readThreshold := readPct
		writeThreshold := readPct + writePct

		// Benchmark SyncInt64Map
		m := NewSyncInt64Map[int64](16)
		for i := 0; i < keyCount/2; i++ {
			m.Set(keys[i], keys[i])
		}
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%keyCount]
				opType := i % totalOps

				if opType < readThreshold {
					_, _ = m.Get(k)
				} else if opType < writeThreshold {
					m.Set(k, k)
				} else {
					m.Del(k)
				}
				i++
			}
		})
	}

	// Different workload patterns
	b.Run("ReadHeavy-90-8-2", func(b *testing.B) {
		benchMap(b, 90, 8, 2)
	})

	b.Run("Balanced-70-20-10", func(b *testing.B) {
		benchMap(b, 70, 20, 10)
	})

	b.Run("WriteHeavy-50-40-10", func(b *testing.B) {
		benchMap(b, 50, 40, 10)
	})
}

// Benchmark for the Has method
func BenchmarkSyncInt64MapHas(b *testing.B) {
	const keyCount = 100_000
	keys := genRealisticKeys(keyCount)

	// Create and populate the map
	m := NewSyncInt64Map[int64](16)
	for i := 0; i < keyCount/2; i++ {
		m.Set(keys[i], keys[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i%keyCount]
			_ = m.Has(k)
			i++
		}
	})
}

// High concurrency benchmark to test scalability
func BenchmarkHighConcurrency(b *testing.B) {
	const keyCount = 10_000
	keys := genRealisticKeys(keyCount)

	m := NewSyncInt64Map[int64](16)
	for i := 0; i < keyCount/2; i++ {
		m.Set(keys[i], keys[i])
	}
	b.ResetTimer()

	// Read-heavy workload (90% reads, 9% writes, 1% deletes)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i%keyCount]
			op := i % 100

			if op < 90 {
				_, _ = m.Get(k)
			} else if op < 99 {
				m.Set(k, k)
			} else {
				m.Del(k)
			}
			i++
		}
	})
}
