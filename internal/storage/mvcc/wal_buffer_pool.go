// WAL buffer pool implementation based on principles from https://github.com/valyala/bytebufferpool
// Portions Copyright (c) 2016 Aliaksandr Valialkin, VertaMedia
// Modified for use in stoolap
//
// This implementation provides an efficient, self-tuning buffer pool that:
// - Automatically adjusts buffer sizes based on actual usage patterns
// - Minimizes memory allocations by reusing buffers
// - Tracks the 95th percentile of buffer sizes to optimize memory usage
// - Prevents memory bloat by limiting maximum buffer size
// - Adapts to changing workloads over time with periodic calibration
package mvcc

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	calibrateCallsThreshold = 4096            // Number of puts before pool calibration
	maxBufferSize           = 8 * 1024 * 1024 // 8MB max buffer size
	minBufferSize           = 256             // Minimum buffer size
	defaultBufferSize       = 64 * 1024       // 64KB default buffer size
)

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
	if size <= minBufferSize {
		return 0
	}

	idx := 0
	for size > minBufferSize {
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
	if calls >= calibrateCallsThreshold {
		// Only calibrate if enough time has passed (every 10 seconds)
		// to avoid excessive CPU usage from constant recalibration
		currentTime := time.Now().UnixNano()
		lastCalibrationTime := p.callTime.Load()
		if currentTime-lastCalibrationTime >= 10*int64(time.Second) {
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
	size := minBufferSize

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
			if maxSize > maxBufferSize {
				maxSize = maxBufferSize
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
		return defaultBufferSize
	}
	return int(size)
}

// Get returns the current max buffer size
func (p *PoolCalibrator) GetMaxSize() int {
	size := p.maxSize.Load()
	if size == 0 {
		return maxBufferSize
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
	calibrator.defaultSize.Store(defaultBufferSize)
	calibrator.maxSize.Store(maxBufferSize)
	calibrator.callTime.Store(time.Now().UnixNano())
}

// WALBufferPool is a global pool of WAL buffers
var WALBufferPool = sync.Pool{
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
	return WALBufferPool.Get().(*ByteBuffer)
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
		WALBufferPool.Put(b)
	}
}
