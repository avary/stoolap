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

const (
	hashBenchSize = 1_000_000 // Number of elements for hash function testing
	mapBenchSize  = 100_000   // Number of elements for map implementation testing
)

// Generate different int64 patterns for testing
func genSequentialInt64(n int) []int64 {
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i)
	}
	return keys
}

func genRandomInt64(n int) []int64 {
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = rand.Int63()
	}
	return keys
}

func genClusteredInt64(n int) []int64 {
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		// Create clusters around multiples of 1000
		base := int64((i / 10) * 1000)
		offset := int64(i % 10)
		keys[i] = base + offset
	}
	return keys
}

func genTimeBasedInt64(n int) []int64 {
	// Simulate timestamp-like keys
	const hourInNanos = int64(60 * 60 * 1000000000)
	keys := make([]int64, n)

	startTime := int64(1609459200000000000) // 2021-01-01 in nanoseconds
	for i := 0; i < n; i++ {
		// Add some randomness but keep general time ordering
		jitter := rand.Int63n(60 * 1000000000) // 60 seconds of jitter
		keys[i] = startTime + int64(i)*hourInNanos + jitter
	}
	return keys
}

// Benchmark the optimized hash function on various key patterns
func BenchmarkHashFunction(b *testing.B) {
	// Test with different key distributions
	keyDistributions := map[string][]int64{
		"Sequential": genSequentialInt64(hashBenchSize),
		"Random":     genRandomInt64(hashBenchSize),
		"Clustered":  genClusteredInt64(hashBenchSize),
		"TimeBased":  genTimeBasedInt64(hashBenchSize),
	}

	// Run benchmarks for each key distribution
	for distribution, keys := range keyDistributions {
		b.Run(distribution, func(b *testing.B) {
			var sink uint64
			for i := 0; i < b.N; i++ {
				k := keys[i%len(keys)]
				sink += hashFast(k)
			}
			// Prevent compiler optimization
			_ = sink
		})
	}
}

// Real-world hash collision benchmark
func BenchmarkHashCollisionHandling(b *testing.B) {
	// Create keys that will have the same hash with a poor hash function
	// but different hashes with a good hash function
	const collisionKeyCount = 1000
	collisionKeys := make([]int64, collisionKeyCount)

	for i := 0; i < collisionKeyCount; i++ {
		// These keys would have identical hashes with a simple hash function
		// but should be distributed well with our optimized hash functions
		collisionKeys[i] = int64(i * 1_000_000)
	}

	m := NewSyncInt64Map[int64](8) // Small size for more collisions

	// Prepopulate
	for i := 0; i < collisionKeyCount; i++ {
		m.Set(collisionKeys[i], collisionKeys[i])
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := collisionKeys[i%collisionKeyCount]
			_, _ = m.Get(k)
			i++
		}
	})
}

// Database-specific workload benchmark
func BenchmarkDatabaseWorkload(b *testing.B) {
	// Simulate database workload with mostly sequential IDs
	// but occasional random access patterns
	const dbSize = 100_000
	dbKeys := make([]int64, dbSize)

	// First half sequential (like auto-incrementing IDs)
	for i := 0; i < dbSize/2; i++ {
		dbKeys[i] = int64(i + 1)
	}

	// Second half with some randomness (like user-supplied IDs)
	for i := dbSize / 2; i < dbSize; i++ {
		dbKeys[i] = rand.Int63()
	}

	// Shuffle slightly to simulate real access patterns
	for i := 0; i < dbSize/10; i++ {
		j := rand.Intn(dbSize)
		k := rand.Intn(dbSize)
		dbKeys[j], dbKeys[k] = dbKeys[k], dbKeys[j]
	}

	runWorkloadBenchmark := func(b *testing.B, readPct, writePct, deletePct int) {
		if readPct+writePct+deletePct != 100 {
			b.Fatalf("Percentages must sum to 100: %d + %d + %d", readPct, writePct, deletePct)
		}

		totalOps := 100
		readThreshold := readPct
		writeThreshold := readPct + writePct

		m := NewSyncInt64Map[int64](16)
		// Prepopulate with half the keys
		for i := 0; i < len(dbKeys)/2; i++ {
			m.Set(dbKeys[i], dbKeys[i])
		}
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := dbKeys[i%len(dbKeys)]
				opType := i % totalOps

				if opType < readThreshold {
					// Read operation
					_, _ = m.Get(k)
				} else if opType < writeThreshold {
					// Write operation
					m.Set(k, k)
				} else {
					// Delete operation
					m.Del(k)
				}
				i++
			}
		})
	}

	b.Run("Read-Heavy-90pct", func(b *testing.B) {
		// 90% reads, 8% writes, 2% deletes
		runWorkloadBenchmark(b, 90, 8, 2)
	})

	b.Run("Balanced-70pct", func(b *testing.B) {
		// 70% reads, 20% writes, 10% deletes
		runWorkloadBenchmark(b, 70, 20, 10)
	})

	b.Run("Write-Heavy-50pct", func(b *testing.B) {
		// 50% reads, 40% writes, 10% deletes
		runWorkloadBenchmark(b, 50, 40, 10)
	})
}
