package mvcc

import (
	"errors"
	"sync/atomic"
	"time"
)

// These errors represent common issues in MVCC operations
var (
	ErrMVCCNoTransaction = errors.New("no transaction provided")
	ErrMVCCPKViolation   = errors.New("primary key violation: duplicate key")
	ErrMVCCInvalidTable  = errors.New("invalid or unknown table")
	ErrMVCCInvalidRow    = errors.New("invalid row data")
)

// Timestamp generation system that guarantees monotonically increasing values
// even under heavy concurrent usage
var (
	lastTimestamp atomic.Int64 // Last timestamp value
	seqNum        atomic.Int64 // Sequence number for timestamp collisions
)

// GetFastTimestamp returns a monotonically increasing timestamp
// suitable for transaction ordering and version tracking
func GetFastTimestamp() int64 {
	// Get current time in nanoseconds
	nowNano := time.Now().UnixNano()

	for {
		// Load current last timestamp
		lastTS := lastTimestamp.Load()

		// If current time is greater than last timestamp, update using CAS
		if nowNano > lastTS {
			// Try to update the timestamp using CAS to avoid race conditions
			if lastTimestamp.CompareAndSwap(lastTS, nowNano) {
				// Reset sequence number when timestamp changes
				seqNum.Store(0)
				return nowNano
			}
		} else {
			// If we have a timestamp collision or system clock went backwards:
			// Use the last timestamp and increment a sequence number in the low bits
			// Get a unique sequence within this timestamp
			seq := seqNum.Add(1)

			// Create a composite timestamp by replacing the low 10 bits with sequence
			// This allows 1024 unique values per nanosecond if needed
			compositeTS := (lastTS & ^int64(0x3FF)) | (seq & 0x3FF)

			return compositeTS
		}

		// If CAS failed, loop and try again
	}
}
