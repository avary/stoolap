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

// Buffer pool implementation based on principles from https://github.com/valyala/bytebufferpool
// Portions Copyright (c) 2016 Aliaksandr Valialkin, VertaMedia
// Modified for use in stoolap
//
// This implementation provides an efficient, self-tuning buffer pool that:
// - Automatically adjusts buffer sizes based on actual usage patterns
// - Minimizes memory allocations by reusing buffers
// - Tracks the 95th percentile of buffer sizes to optimize memory usage
// - Prevents memory bloat by limiting maximum buffer size
// - Adapts to changing workloads over time with periodic calibration
package common

import (
	"sync"
	"sync/atomic"
	"time"
)

// BufferPoolConfig contains configuration options for the buffer pool
type BufferPoolConfig struct {
	// CalibrateCallsThreshold is the number of puts before pool calibration
	CalibrateCallsThreshold uint64
	// MaxBufferSize is the maximum size of buffers to keep in the pool
	MaxBufferSize int
	// MinBufferSize is the minimum size buffer to use
	MinBufferSize int
	// DefaultBufferSize is the default size for new buffers
	DefaultBufferSize int
	// CalibrationInterval is the minimum time between calibrations in seconds
	CalibrationInterval int64
}

// DefaultBufferPoolConfig provides the default configuration values
var DefaultBufferPoolConfig = BufferPoolConfig{
	CalibrateCallsThreshold: 4096,
	MaxBufferSize:           8 * 1024 * 1024, // 8MB max buffer size
	MinBufferSize:           256,             // Minimum buffer size
	DefaultBufferSize:       64 * 1024,       // 64KB default buffer size
	CalibrationInterval:     10,              // 10 seconds between calibrations
}

// Current configuration used by the buffer pool
var currentConfig = DefaultBufferPoolConfig

// PoolCalibrator manages buffer size calibration for optimal memory utilization
type PoolCalibrator struct {
	calls       atomic.Uint64
	defaultSize atomic.Uint64
	maxSize     atomic.Uint64

	// Calibration statistics
	callTime atomic.Int64 // Last calibration time
	sizes    sync.Pool    // Pool for int slices used during calibration
	counters [20]uint64   // Track buffer size distribution (exponential size classes)
}

func (p *PoolCalibrator) getSizeIndex(size int) int {
	if size <= currentConfig.MinBufferSize {
		return 0
	}

	idx := 0
	for size > currentConfig.MinBufferSize {
		size >>= 1
		idx++
		if idx >= len(p.counters)-1 {
			break
		}
	}
	return idx
}

// Put records buffer size statistics for calibration
func (p *PoolCalibrator) Put(size int) {
	// Record statistics for calibration
	idx := p.getSizeIndex(size)
	atomic.AddUint64(&p.counters[idx], 1)

	// Calibrate if enough calls have been made
	calls := p.calls.Add(1)
	if calls >= currentConfig.CalibrateCallsThreshold {
		// Only calibrate if enough time has passed (according to CalibrationInterval)
		// to avoid excessive CPU usage from constant recalibration
		currentTime := time.Now().UnixNano()
		lastCalibrationTime := p.callTime.Load()
		if currentTime-lastCalibrationTime >= currentConfig.CalibrationInterval*int64(time.Second) {
			// Try to swap the time, but only allow one goroutine to calibrate
			if p.callTime.CompareAndSwap(lastCalibrationTime, currentTime) {
				p.calibrate()
				p.calls.Store(0)
			}
		}
	}
}

// calibrate analyzes buffer size usage and adjusts default and max size
func (p *PoolCalibrator) calibrate() {
	// Calculate distribution and find the 95th percentile size
	total := uint64(0)
	for _, count := range p.counters {
		total += count
	}

	// If insufficient data, don't adjust
	if total == 0 {
		return
	}

	// Find 95th percentile size
	threshold := total * 95 / 100
	cumulative := uint64(0)
	size := currentConfig.MinBufferSize

	for i := 0; i < len(p.counters); i++ {
		if i > 0 {
			size *= 2
		}
		cumulative += p.counters[i]

		if cumulative >= threshold {
			// Adjust default size based on 95th percentile
			p.defaultSize.Store(uint64(size))

			// Set max size to 2x the 95th percentile, capped at maxBufferSize
			maxSize := size * 2
			if maxSize > currentConfig.MaxBufferSize {
				maxSize = currentConfig.MaxBufferSize
			}
			p.maxSize.Store(uint64(maxSize))

			// Reset counters for next calibration period
			for j := 0; j < len(p.counters); j++ {
				p.counters[j] = 0
			}
			return
		}
	}
}

// Get returns the current default buffer size
func (p *PoolCalibrator) GetDefaultSize() int {
	size := p.defaultSize.Load()
	if size == 0 {
		return currentConfig.DefaultBufferSize
	}
	return int(size)
}

// Get returns the current max buffer size
func (p *PoolCalibrator) GetMaxSize() int {
	size := p.maxSize.Load()
	if size == 0 {
		return currentConfig.MaxBufferSize
	}
	return int(size)
}

// Global calibrator for buffer sizes
var calibrator = &PoolCalibrator{
	sizes: sync.Pool{
		New: func() interface{} {
			return make([]int, 0, 1024)
		},
	},
}

// Initialize the calibrator with default values
func init() {
	calibrator.defaultSize.Store(uint64(currentConfig.DefaultBufferSize))
	calibrator.maxSize.Store(uint64(currentConfig.MaxBufferSize))
	calibrator.callTime.Store(time.Now().UnixNano())
}

// ConfigureBufferPool allows customizing the buffer pool settings
// This should be called early in the application startup before
// the buffer pool is heavily used
func ConfigureBufferPool(config BufferPoolConfig) {
	// Apply sensible minimum values
	if config.MinBufferSize < 64 {
		config.MinBufferSize = 64
	}

	if config.DefaultBufferSize < config.MinBufferSize {
		config.DefaultBufferSize = config.MinBufferSize
	}

	if config.MaxBufferSize < config.DefaultBufferSize {
		config.MaxBufferSize = config.DefaultBufferSize * 2
	}

	if config.CalibrationInterval < 1 {
		config.CalibrationInterval = 1
	}

	if config.CalibrateCallsThreshold < 100 {
		config.CalibrateCallsThreshold = 100
	}

	// Apply the new configuration
	currentConfig = config

	// Reset the calibrator with the new default values
	calibrator.defaultSize.Store(uint64(config.DefaultBufferSize))
	calibrator.maxSize.Store(uint64(config.MaxBufferSize))

	// Reset the counters and call count
	for i := range calibrator.counters {
		calibrator.counters[i] = 0
	}
	calibrator.calls.Store(0)
}

// BufferPool is a global pool of WAL buffers
var BufferPool = sync.Pool{
	New: func() interface{} {
		return &ByteBuffer{
			B: make([]byte, 0, calibrator.GetDefaultSize()),
		}
	},
}

// ByteBuffer is a resizable byte buffer with optimized handling
type ByteBuffer struct {
	B []byte // Underlying byte slice
}

// Reset resets the buffer to be empty but retains allocated memory
func (b *ByteBuffer) Reset() {
	b.B = b.B[:0]
}

// Write implements io.Writer by appending data to the buffer
func (b *ByteBuffer) Write(p []byte) (int, error) {
	b.B = append(b.B, p...)
	return len(p), nil
}

// WriteString appends a string to the buffer
func (b *ByteBuffer) WriteString(s string) (int, error) {
	b.B = append(b.B, s...)
	return len(s), nil
}

// Len returns the current buffer length
func (b *ByteBuffer) Len() int {
	return len(b.B)
}

// Bytes returns a slice of the buffer's contents
func (b *ByteBuffer) Bytes() []byte {
	return b.B
}

// Get returns a buffer from the pool
func GetBufferPool() *ByteBuffer {
	return BufferPool.Get().(*ByteBuffer)
}

// BufferPoolStats contains statistics about the buffer pool usage
type BufferPoolStats struct {
	// Current default buffer size
	DefaultBufferSize int
	// Current maximum buffer size
	MaxBufferSize int
	// Configuration used by the pool
	Config BufferPoolConfig
	// Total number of buffer puts recorded
	TotalCalls uint64
}

// Get statistics about the buffer pool
func GetBufferPoolStats() BufferPoolStats {
	return BufferPoolStats{
		DefaultBufferSize: calibrator.GetDefaultSize(),
		MaxBufferSize:     calibrator.GetMaxSize(),
		Config:            currentConfig,
		TotalCalls:        calibrator.calls.Load(),
	}
}

// Put returns a buffer to the pool
func PutBufferPool(b *ByteBuffer) {
	if b == nil {
		return
	}

	// Record the buffer size for calibration
	size := cap(b.B)
	calibrator.Put(size)

	// Only return reasonably sized buffers to the pool
	maxSize := calibrator.GetMaxSize()
	if size <= maxSize {
		b.Reset()
		BufferPool.Put(b)
	}
}
