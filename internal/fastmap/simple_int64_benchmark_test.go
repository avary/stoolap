package fastmap

import (
	"testing"
)

// BenchmarkSimpleMapOperations benchmarks both maps with simple operations
func BenchmarkSimpleMapOperations(b *testing.B) {
	// Generate sequential keys
	const size = 10000
	keys := make([]int64, size)
	for i := range keys {
		keys[i] = int64(i)
	}

	// Benchmark Put operations
	b.Run("Put", func(b *testing.B) {
		b.Run("Int64Map", func(b *testing.B) {
			m := NewInt64Map[int64](size)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				m.Put(key, key)
			}
		})

		b.Run("StdMap", func(b *testing.B) {
			m := make(map[int64]int64, size)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				m[key] = key
			}
		})
	})

	// Benchmark Get operations
	b.Run("Get", func(b *testing.B) {
		fastMap := NewInt64Map[int64](size)
		stdMap := make(map[int64]int64, size)

		// Populate maps
		for i := 0; i < size; i++ {
			fastMap.Put(keys[i], keys[i])
			stdMap[keys[i]] = keys[i]
		}

		b.Run("Int64Map", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				_, _ = fastMap.Get(key)
			}
		})

		b.Run("StdMap", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				_ = stdMap[key]
			}
		})
	})

	// Benchmark Has operation
	b.Run("Has", func(b *testing.B) {
		fastMap := NewInt64Map[int64](size)

		// Populate maps
		for i := 0; i < size; i++ {
			fastMap.Put(keys[i], keys[i])
		}

		b.Run("Int64Map", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				_ = fastMap.Has(key)
			}
		})
	})

	// Benchmark Delete operation
	b.Run("Delete", func(b *testing.B) {
		b.Run("Int64Map", func(b *testing.B) {
			// Generate a fresh map for each run
			m := NewInt64Map[int64](size)

			// Populate map
			for i := 0; i < size; i++ {
				m.Put(keys[i], keys[i])
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				m.Del(key)
			}
		})

		b.Run("StdMap", func(b *testing.B) {
			// Generate a fresh map for each run
			m := make(map[int64]int64, size)

			// Populate map
			for i := 0; i < size; i++ {
				m[keys[i]] = keys[i]
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%size]
				delete(m, key)
			}
		})
	})
}
