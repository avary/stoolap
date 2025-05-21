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
	"sync"
	"testing"
)

func BenchmarkMapTypes(b *testing.B) {
	// Key distribution types
	const (
		Sequential  = "sequential"
		Random      = "random"
		Clustered   = "clustered"
		FewHotKeys  = "few_hot_keys"
		ManyHotKeys = "many_hot_keys"
	)

	// Operation mix types
	const (
		ReadHeavy  = "read_heavy"  // 80% read, 15% write, 5% delete
		WriteHeavy = "write_heavy" // 20% read, 70% write, 10% delete
		Balanced   = "balanced"    // 50% read, 45% write, 5% delete
		ReadOnly   = "read_only"   // 100% read
		WriteOnly  = "write_only"  // 100% write
	)

	// Map sizes
	sizes := []int{
		1_000,     // Small
		100_000,   // Medium
		1_000_000, // Large
	}

	// Segment powers to test
	segmentPowers := []uint8{4, 6, 8} // 16, 64, 256 segments

	// Prepare keys for each distribution
	prepareKeys := func(size int, distribution string) []int64 {
		keys := make([]int64, size)

		switch distribution {
		case Sequential:
			for i := 0; i < size; i++ {
				keys[i] = int64(i)
			}
		case Random:
			for i := 0; i < size; i++ {
				// Simple xorshift for pseudorandom numbers
				x := int64(i * 0x9E3779B9)
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				keys[i] = x
			}
		case Clustered:
			cluster := int64(0)
			for i := 0; i < size; i++ {
				if i%100 == 0 {
					cluster = int64(i/100) * 1000
				}
				keys[i] = cluster + int64(i%100)
			}
		case FewHotKeys:
			hotKeys := [10]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			for i := 0; i < size; i++ {
				if i%5 == 0 { // 20% hot keys
					keys[i] = hotKeys[i%10]
				} else {
					keys[i] = int64(i) + 100
				}
			}
		case ManyHotKeys:
			numHotKeys := 100
			hotKeys := make([]int64, numHotKeys)
			for i := 0; i < numHotKeys; i++ {
				hotKeys[i] = int64(i) + 1000
			}
			for i := 0; i < size; i++ {
				if i%2 == 0 { // 50% hot keys
					keys[i] = hotKeys[i%numHotKeys]
				} else {
					keys[i] = int64(i) + 10000
				}
			}
		}

		return keys
	}

	// Configure goroutines for testing
	concurrencies := []int{1, 4, 16, 64} // Number of goroutines

	// Run benchmarks with all combinations
	for _, size := range sizes {
		for _, distribution := range []string{Sequential, Random, Clustered, FewHotKeys, ManyHotKeys} {
			keys := prepareKeys(size, distribution)

			for _, opMix := range []string{ReadHeavy, WriteHeavy, Balanced, ReadOnly, WriteOnly} {
				for _, concurrency := range concurrencies {
					benchName := func(mapType string) string {
						return mapType + "/" +
							distribution + "/" +
							opMix + "/" +
							"size_" + intToStr(size) + "/" +
							"conc_" + intToStr(concurrency)
					}

					// Skip single-threaded benchmarks for large maps to save time
					if size == 1_000_000 && concurrency == 1 {
						continue
					}

					// Prepare operation probabilities based on mix
					var readProb, writeProb int
					switch opMix {
					case ReadHeavy:
						readProb, writeProb = 80, 15
						// deleteProb = 5
					case WriteHeavy:
						readProb, writeProb = 20, 70
						// deleteProb = 10
					case Balanced:
						readProb, writeProb = 50, 45
						// deleteProb = 5
					case ReadOnly:
						readProb, writeProb = 100, 0
						// deleteProb = 0
					case WriteOnly:
						readProb, writeProb = 0, 100
						// deleteProb = 0
					}

					// Benchmark for regular Int64Map (only single-threaded)
					if concurrency == 1 {
						b.Run(benchName("Int64Map"), func(b *testing.B) {
							m := NewInt64Map[int](size)

							// Pre-populate with half the keys
							for i := 0; i < size/2; i++ {
								m.Put(keys[i], i)
							}

							b.ResetTimer()

							for i := 0; i < b.N; i++ {
								keyIdx := i % size
								opChoice := i % 100

								if opChoice < readProb {
									// Read operation
									_, _ = m.Get(keys[keyIdx])
								} else if opChoice < readProb+writeProb {
									// Write operation
									m.Put(keys[keyIdx], keyIdx)
								} else {
									// Delete operation
									_ = m.Del(keys[keyIdx])
								}
							}
						})
					}

					// Benchmark for SyncInt64Map
					b.Run(benchName("SyncInt64Map"), func(b *testing.B) {
						sizePower := uint(0)
						for 1<<sizePower < size {
							sizePower++
						}
						m := NewSyncInt64Map[int](sizePower)

						// Pre-populate with half the keys
						for i := 0; i < size/2; i++ {
							m.Set(keys[i], i)
						}

						b.ResetTimer()

						var wg sync.WaitGroup
						opsPerGoroutine := b.N / concurrency

						for g := 0; g < concurrency; g++ {
							wg.Add(1)
							go func(goroutineID int) {
								defer wg.Done()

								startIdx := goroutineID * opsPerGoroutine
								endIdx := startIdx + opsPerGoroutine
								if goroutineID == concurrency-1 {
									endIdx = b.N // Last goroutine takes any remainder
								}

								for i := startIdx; i < endIdx; i++ {
									keyIdx := i % size
									opChoice := i % 100

									if opChoice < readProb {
										// Read operation
										_, _ = m.Get(keys[keyIdx])
									} else if opChoice < readProb+writeProb {
										// Write operation
										m.Set(keys[keyIdx], keyIdx)
									} else {
										// Delete operation
										_ = m.Del(keys[keyIdx])
									}
								}
							}(g)
						}

						wg.Wait()
					})

					// Benchmark for SegmentInt64Map with different segment powers
					for _, segPower := range segmentPowers {
						segName := benchName("SegmentInt64Map_" + intToStr(int(1<<segPower)))

						b.Run(segName, func(b *testing.B) {
							m := NewSegmentInt64Map[int](segPower, size)

							// Pre-populate with half the keys
							for i := 0; i < size/2; i++ {
								m.Set(keys[i], i)
							}

							b.ResetTimer()

							var wg sync.WaitGroup
							opsPerGoroutine := b.N / concurrency

							for g := 0; g < concurrency; g++ {
								wg.Add(1)
								go func(goroutineID int) {
									defer wg.Done()

									startIdx := goroutineID * opsPerGoroutine
									endIdx := startIdx + opsPerGoroutine
									if goroutineID == concurrency-1 {
										endIdx = b.N // Last goroutine takes any remainder
									}

									for i := startIdx; i < endIdx; i++ {
										keyIdx := i % size
										opChoice := i % 100

										if opChoice < readProb {
											// Read operation
											_, _ = m.Get(keys[keyIdx])
										} else if opChoice < readProb+writeProb {
											// Write operation
											m.Set(keys[keyIdx], keyIdx)
										} else {
											// Delete operation
											_ = m.Del(keys[keyIdx])
										}
									}
								}(g)
							}

							wg.Wait()
						})
					}
				}
			}
		}
	}
}

func intToStr(n int) string {
	switch {
	case n >= 1_000_000:
		return string([]byte{'0' + byte(n/1_000_000)}) + "M"
	case n >= 1_000:
		return string([]byte{'0' + byte(n/1_000)}) + "K"
	default:
		// Simple conversion for small numbers
		if n == 0 {
			return "0"
		}

		var buf [10]byte
		pos := len(buf)

		for n > 0 {
			pos--
			buf[pos] = byte('0' + n%10)
			n /= 10
		}

		return string(buf[pos:])
	}
}
