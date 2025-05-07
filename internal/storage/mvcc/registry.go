package mvcc

import (
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/fastmap"
)

// IsolationLevel represents the transaction isolation level
type IsolationLevel int

const (
	// ReadCommitted is the isolation level where transactions see only committed data
	ReadCommitted IsolationLevel = iota
	// SnapshotIsolation (equivalent to Repeatable Read) ensures transactions see a consistent
	// snapshot of the database as it existed at the start of the transaction
	SnapshotIsolation
)

// TransactionRegistry manages transaction states and visibility rules
// Lock-free implementation using our optimized SyncInt64Map for optimal performance and concurrency
type TransactionRegistry struct {
	nextTxnID             atomic.Int64
	activeTransactions    *fastmap.SyncInt64Map[int64] // txnID -> begin timestamp
	committedTransactions *fastmap.SyncInt64Map[int64] // txnID -> commit timestamp
	isolationLevel        IsolationLevel
	accepting             atomic.Bool // Flag to control if new transactions are accepted
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry() *TransactionRegistry {
	reg := &TransactionRegistry{
		activeTransactions:    fastmap.NewSyncInt64Map[int64](16),
		committedTransactions: fastmap.NewSyncInt64Map[int64](16),
		isolationLevel:        ReadCommitted, // Default isolation level
	}
	reg.accepting.Store(true) // Start accepting transactions by default
	return reg
}

// SetIsolationLevel sets the isolation level for this registry
func (r *TransactionRegistry) SetIsolationLevel(level IsolationLevel) {
	r.isolationLevel = level
}

// GetIsolationLevel returns the current isolation level
func (r *TransactionRegistry) GetIsolationLevel() IsolationLevel {
	return r.isolationLevel
}

// BeginTransaction starts a new transaction
func (r *TransactionRegistry) BeginTransaction() (txnID int64, beginTS int64) {
	// Check if we're accepting new transactions
	if !r.accepting.Load() {
		// Return negative txnID to indicate error condition
		return -1, 0
	}

	// Generate a new transaction ID atomically
	txnID = r.nextTxnID.Add(1)
	beginTS = time.Now().UnixNano()

	// Record the transaction (thread-safe with SyncInt64Map)
	r.activeTransactions.Set(txnID, beginTS)

	return txnID, beginTS
}

// CommitTransaction commits a transaction
func (r *TransactionRegistry) CommitTransaction(txnID int64) (commitTS int64) {
	commitTS = time.Now().UnixNano()

	// Lock-free operations with SyncInt64Map
	r.committedTransactions.Set(txnID, commitTS)
	r.activeTransactions.Del(txnID)

	return commitTS
}

// RecoverCommittedTransaction recreates a committed transaction during recovery
func (r *TransactionRegistry) RecoverCommittedTransaction(txnID int64, commitTS int64) {
	// During recovery, we directly add the transaction to the committed map
	// with its original commit timestamp
	r.committedTransactions.Set(txnID, commitTS)

	// Also update nextTxnID if necessary to ensure new transactions get unique IDs
	for {
		current := r.nextTxnID.Load()
		if txnID >= current {
			if r.nextTxnID.CompareAndSwap(current, txnID+1) {
				break
			}
			// If CAS failed, another thread updated it, try again
		} else {
			// Current ID is already higher, nothing to do
			break
		}
	}
}

// RecoverAbortedTransaction records an aborted transaction during recovery
func (r *TransactionRegistry) RecoverAbortedTransaction(txnID int64) {
	// We don't need to explicitly track aborted transactions
	// But we do need to update nextTxnID to avoid ID conflicts
	for {
		current := r.nextTxnID.Load()
		if txnID >= current {
			if r.nextTxnID.CompareAndSwap(current, txnID+1) {
				break
			}
			// If CAS failed, another thread updated it, try again
		} else {
			// Current ID is already higher, nothing to do
			break
		}
	}
}

// AbortTransaction marks a transaction as aborted
func (r *TransactionRegistry) AbortTransaction(txnID int64) {
	// Lock-free delete with SyncInt64Map
	r.activeTransactions.Del(txnID)
	// No entry in committedTransactions means it was aborted
}

// GetCommitTimestamp gets the commit timestamp for a transaction
func (r *TransactionRegistry) GetCommitTimestamp(txnID int64) (int64, bool) {
	// Thread-safe get with SyncInt64Map
	return r.committedTransactions.Get(txnID)
}

// IsDirectlyVisible is an optimized version that only checks common cases
// for better performance in bulk operations. It only returns true for
// already committed transactions (in ReadCommitted mode).
func (r *TransactionRegistry) IsDirectlyVisible(versionTxnID int64) bool {
	// Special case for recovery transactions with ID = -1
	// These are always visible to everyone
	if versionTxnID == -1 {
		return true
	}

	// Fast path for ReadCommitted isolation level (the default)
	// where any committed transaction is visible to all other transactions
	if r.isolationLevel == ReadCommitted {
		// Thread-safe check with SyncInt64Map
		// This is a hot path that benefits from being as fast as possible
		_, committed := r.committedTransactions.Get(versionTxnID)
		return committed
	}

	// For other isolation levels, we need full visibility check
	return false
}

// IsVisible determines if a row version is visible to a transaction
func (r *TransactionRegistry) IsVisible(versionTxnID int64, viewerTxnID int64) bool {
	// Special case for recovery transactions with ID = -1
	// These are always visible to everyone
	if versionTxnID == -1 {
		return true
	}

	// Ultra-fast path for reading own writes
	// This optimization helps performance for update/delete operations that read then write
	if versionTxnID == viewerTxnID {
		// Current transaction's own changes are always visible
		return true
	}

	// Fast path for common READ COMMITTED level (most databases default to this)
	if r.isolationLevel == ReadCommitted {
		// In READ COMMITTED, only committed transactions are visible
		// This delegation is inlinable and very efficient
		return r.IsDirectlyVisible(versionTxnID)
	}

	// For SNAPSHOT isolation, we need full visibility check
	// All operations are thread-safe with SyncInt64Map

	// Transaction can only see committed changes from other transactions
	// Lock-free access via SyncInt64Map
	commitTS, committed := r.committedTransactions.Get(versionTxnID)
	if !committed {
		// Not committed, definitely not visible
		return false
	}

	// For Snapshot Isolation, version must be committed before viewer began
	// Lock-free access via SyncInt64Map
	viewerBeginTS, viewerActive := r.activeTransactions.Get(viewerTxnID)
	if !viewerActive {
		// Viewer transaction isn't active, use its commit time
		// Lock-free access via SyncInt64Map
		viewerBeginTS, committed = r.committedTransactions.Get(viewerTxnID)
		if !committed {
			// If viewer isn't committed or active, it must be aborted
			return false
		}
	}

	// Version must have been committed before or at the same time viewer began
	// This timestamp comparison is the core of snapshot isolation
	return commitTS <= viewerBeginTS
}

// CleanupOldTransactions removes committed transactions older than maxAge
// This helps prevent memory leaks from accumulating transaction records
func (r *TransactionRegistry) CleanupOldTransactions(maxAge time.Duration) int {
	// Calculate the cutoff timestamp for old transactions
	cutoffTime := time.Now().Add(-maxAge).UnixNano()

	// Get the list of all active transaction IDs to avoid cleaning up
	// transactions that might still be needed for visibility checks
	var activeSet map[int64]struct{}

	// If we're in snapshot isolation mode, we need to preserve transactions
	// that might still be visible to active transactions
	if r.isolationLevel == SnapshotIsolation {
		activeSet = make(map[int64]struct{})
		// Get all active transactions using ForEach
		r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
			activeSet[txnID] = struct{}{}
			return true
		})
	}

	// Track how many transactions we remove
	removed := 0

	// Clean up old committed transactions using ForEach
	// This is safe because SyncInt64Map is already thread-safe
	r.committedTransactions.ForEach(func(txnID, commitTS int64) bool {
		// Skip transactions that are still active
		if r.isolationLevel == SnapshotIsolation {
			if _, isActive := activeSet[txnID]; isActive {
				return true
			}
		}

		// Only remove transactions older than the cutoff
		if commitTS < cutoffTime {
			r.committedTransactions.Del(txnID)
			removed++
		}
		return true
	})

	return removed
}

// WaitForActiveTransactions waits for all active transactions to complete with timeout
// Returns the number of transactions that were still active after timeout
func (r *TransactionRegistry) WaitForActiveTransactions(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)

	for {
		// Check if we've reached the timeout
		if time.Now().After(deadline) {
			break
		}

		// Count active transactions - SyncInt64Map is already thread-safe
		activeCount := 0
		r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
			activeCount++
			return true
		})

		// If no active transactions, we're done
		if activeCount == 0 {
			return 0
		}

		// Wait a short time before checking again
		time.Sleep(10 * time.Millisecond)
	}

	// Return the number of transactions still active after timeout
	activeCount := 0
	r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
		activeCount++
		return true
	})

	return activeCount
}

// StopAcceptingTransactions stops the registry from accepting new transactions
func (r *TransactionRegistry) StopAcceptingTransactions() {
	r.accepting.Store(false)
}
