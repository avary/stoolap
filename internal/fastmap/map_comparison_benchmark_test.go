package fastmap

import (
	"sync"
	"testing"
)

// BenchmarkMapsComparison runs a series of benchmarks comparing the performance
// of different map implementations under various conditions
func BenchmarkMapsComparison(b *testing.B) {
	// We'll test with different concurrency levels
	concurrencies := []int{1, 4, 8, 16}

	// Test with sequential keys (common in database systems)
	benchmarkMapImplementations(b, "Sequential", generateSequentialKeys, concurrencies)

	// Test with random keys
	benchmarkMapImplementations(b, "Random", generateRandomKeys, concurrencies)

	// Test with clustered keys (common in time-series data)
	benchmarkMapImplementations(b, "Clustered", generateClusteredKeys, concurrencies)
}

// Key generation functions
func generateSequentialKeys(n int) []int64 {
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i)
	}
	return keys
}

func generateRandomKeys(n int) []int64 {
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		x := int64(i * 0x9E3779B9)
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		keys[i] = x
	}
	return keys
}

func generateClusteredKeys(n int) []int64 {
	keys := make([]int64, n)
	cluster := int64(0)
	for i := 0; i < n; i++ {
		if i%100 == 0 {
			cluster = int64(i/100) * 1000
		}
		keys[i] = cluster + int64(i%100)
	}
	return keys
}

// Main benchmark function
func benchmarkMapImplementations(b *testing.B, keyType string, keyGen func(int) []int64, concurrencies []int) {
	const mapSize = 100_000

	// Generate keys
	keys := keyGen(mapSize)

	// Benchmark each implementation with different concurrency patterns
	for _, concurrency := range concurrencies {
		// Only test Int64Map with single thread (it's not thread-safe)
		if concurrency == 1 {
			b.Run(keyType+"/Int64Map/Conc=1", func(b *testing.B) {
				m := NewInt64Map[int](mapSize)

				// Pre-populate with half the keys
				for i := 0; i < mapSize/2; i++ {
					m.Put(keys[i], i)
				}

				b.ResetTimer()

				// Run benchmark
				for i := 0; i < b.N; i++ {
					idx := i % mapSize
					op := i % 10

					switch {
					case op < 8: // 80% reads
						_, _ = m.Get(keys[idx])
					case op < 9: // 10% writes
						m.Put(keys[idx], idx)
					default: // 10% deletes
						_ = m.Del(keys[idx])
					}
				}
			})
		}

		// Benchmark SyncInt64Map
		b.Run(keyType+"/SyncInt64Map/Conc="+intToStr(concurrency), func(b *testing.B) {
			// Calculate power of 2 size
			sizePower := uint(0)
			for 1<<sizePower < mapSize {
				sizePower++
			}
			m := NewSyncInt64Map[int](sizePower)

			// Pre-populate with half the keys
			for i := 0; i < mapSize/2; i++ {
				m.Set(keys[i], i)
			}

			b.ResetTimer()

			var wg sync.WaitGroup
			perG := b.N / concurrency

			for g := 0; g < concurrency; g++ {
				wg.Add(1)
				go func(gID int) {
					defer wg.Done()

					start := gID * perG
					end := start + perG
					if gID == concurrency-1 {
						end = b.N // Last goroutine takes any remainder
					}

					for i := start; i < end; i++ {
						idx := i % mapSize
						op := i % 10

						switch {
						case op < 8: // 80% reads
							_, _ = m.Get(keys[idx])
						case op < 9: // 10% writes
							m.Set(keys[idx], idx)
						default: // 10% deletes
							_ = m.Del(keys[idx])
						}
					}
				}(g)
			}

			wg.Wait()
		})

		// Benchmark SegmentInt64Map with different segment counts
		for _, segPower := range []uint8{4, 6, 8} { // 16, 64, 256 segments
			segCount := 1 << segPower
			name := keyType + "/SegmentInt64Map_" + intToStr(segCount) + "/Conc=" + intToStr(concurrency)

			b.Run(name, func(b *testing.B) {
				m := NewSegmentInt64Map[int](segPower, mapSize)

				// Pre-populate with half the keys
				for i := 0; i < mapSize/2; i++ {
					m.Set(keys[i], i)
				}

				b.ResetTimer()

				var wg sync.WaitGroup
				perG := b.N / concurrency

				for g := 0; g < concurrency; g++ {
					wg.Add(1)
					go func(gID int) {
						defer wg.Done()

						start := gID * perG
						end := start + perG
						if gID == concurrency-1 {
							end = b.N // Last goroutine takes any remainder
						}

						for i := start; i < end; i++ {
							idx := i % mapSize
							op := i % 10

							switch {
							case op < 8: // 80% reads
								_, _ = m.Get(keys[idx])
							case op < 9: // 10% writes
								m.Set(keys[idx], idx)
							default: // 10% deletes
								_ = m.Del(keys[idx])
							}
						}
					}(g)
				}

				wg.Wait()
			})
		}
	}
}
