package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// RowVersion represents a specific version of a row with complete data
type RowVersion struct {
	TxnID      int64       // Transaction that created this version
	IsDeleted  bool        // Whether this is a deletion marker
	Data       storage.Row // Complete row data, not just a reference
	RowID      int64       // Row identifier (replaces string primary key)
	CreateTime int64       // Timestamp when this version was created
}

// VersionStore tracks the latest committed version of each row for a table
// Simplified to keep only one version per row (the latest committed version)
type VersionStore struct {
	versions  *fastmap.FastInt64Map[*RowVersion] // Using high-performance concurrent map
	tableName string                             // The name of the table this store belongs to

	// Columnar indexes provide HTAP capabilities
	columnarIndexes map[string]storage.Index
	columnarMutex   sync.RWMutex
	versionsMu      sync.RWMutex // For version operations

	closed atomic.Bool // Whether this store has been closed - using atomic for better performance

	// Reference to the engine that owns this version store
	engine *MVCCEngine // Engine that owns this version store
}

// NewVersionStore creates a new version store
func NewVersionStore(tableName string, engine *MVCCEngine) *VersionStore {
	vs := &VersionStore{
		versions:        fastmap.NewFastInt64Map[*RowVersion](16), // Start with reasonable capacity
		tableName:       tableName,
		columnarIndexes: make(map[string]storage.Index),
		engine:          engine,
	}
	// Initialize atomic.Bool to false (not closed)
	vs.closed.Store(false)
	return vs
}

// AddVersion adds or replaces the version for a row
// Only keeps the latest committed version per row
func (vs *VersionStore) AddVersion(rowID int64, version RowVersion) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return // Skip version update if closed
	}

	// For better efficiency, we can reuse existing version objects
	rv, exists := vs.versions.Get(rowID)
	if !exists {
		// Check again if closed after the potentially expensive lookup
		if vs.closed.Load() {
			return
		}

		// Create a new RowVersion and store a pointer to it
		newVersion := &RowVersion{
			TxnID:      version.TxnID,
			IsDeleted:  version.IsDeleted,
			Data:       version.Data,
			RowID:      version.RowID,
			CreateTime: version.CreateTime,
		}
		vs.versions.Set(rowID, newVersion)

		// Update columnar indexes with the new version
		vs.UpdateColumnarIndexes(rowID, version)

		// Record operation in WAL if persistence is enabled and this isn't a recovery operation
		if vs.engine != nil && vs.engine.persistence != nil &&
			vs.engine.persistence.IsEnabled() && version.TxnID >= 0 {
			// Only write to WAL if this is a committed transaction
			if vs.engine.registry.IsDirectlyVisible(version.TxnID) {
				// Use the safer async helper method which handles all the details
				err := vs.engine.persistence.RecordDMLOperation(version.TxnID, vs.tableName, rowID, version)
				if err != nil {
					// Handle error if needed
					fmt.Printf("Error: recording DML operation: %v\n", err)
				}
			}
		}

	} else {
		// Store old deleted status for index updates
		oldIsDeleted := rv.IsDeleted
		oldTxnID := rv.TxnID

		// If we already have a version for this row and we're replacing it,
		// we need to clean up any references it might have to avoid memory leaks
		if rv.Data != nil {
			// Clear references to help garbage collection
			rv.Data = nil
		}

		// Update the fields of the existing version
		rv.TxnID = version.TxnID
		rv.IsDeleted = version.IsDeleted
		rv.Data = version.Data
		rv.RowID = version.RowID
		rv.CreateTime = version.CreateTime

		// Update columnar indexes
		// First check if there are any indexes to update
		vs.columnarMutex.RLock()
		hasIndexes := len(vs.columnarIndexes) > 0
		vs.columnarMutex.RUnlock()

		if hasIndexes {
			// If the row was previously not deleted but is now deleted,
			// we need to remove it from all indexes
			if !oldIsDeleted && version.IsDeleted {
				vs.UpdateColumnarIndexes(rowID, version)
			} else if oldIsDeleted && !version.IsDeleted {
				vs.UpdateColumnarIndexes(rowID, version)
			} else {
				// Always update indexes as we can't directly compare slices
				vs.UpdateColumnarIndexes(rowID, version)
			}
		}

		// Record update in WAL if persistence is enabled and this isn't a recovery operation
		// Only consider this for actual changes by real transactions
		if vs.engine != nil && vs.engine.persistence != nil &&
			vs.engine.persistence.IsEnabled() && version.TxnID >= 0 && oldTxnID != version.TxnID {
			// Only write to WAL if this is a committed transaction
			if vs.engine.registry.IsDirectlyVisible(version.TxnID) {
				// Use the safer async helper method which handles all the details
				err := vs.engine.persistence.RecordDMLOperation(version.TxnID, vs.tableName, rowID, version)
				if err != nil {
					// Handle error if needed
					fmt.Printf("Error: recording DML operation: %v\n", err)
				}
			}
		}
	}
}

// QuickCheckRowExistence is a fast check if a row might exist
// This is optimized for the critical path in Insert operation
// Returns false if the row definitely doesn't exist
func (vs *VersionStore) QuickCheckRowExistence(rowID int64) bool {
	// Check if the version store is closed
	if vs.closed.Load() {
		return false
	}

	// Check in-memory store first - no lock needed with haxmap
	_, exists := vs.versions.Get(rowID)
	if exists {
		return true
	}

	// If not in memory and persistence is enabled, check disk store
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Quick index-only check in disk store
			return diskStore.QuickCheckRowExists(rowID)
		}
	}

	return false
}

// GetVisibleVersion gets the latest visible version of a row
func (vs *VersionStore) GetVisibleVersion(rowID int64, txnID int64) (RowVersion, bool) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return RowVersion{}, false
	}

	// Check if the row exists in memory - haxmap is concurrency-safe
	versionPtr, exists := vs.versions.Get(rowID)
	if exists {
		// With a single version per row, check if that version is visible
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			// Return a copy of the version by value
			return *versionPtr, true
		}
		// If version exists but isn't visible, return false directly
		// We don't need to check disk because memory version is newer
		return RowVersion{}, false
	}

	// If not in memory and persistence is enabled, check disk store
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Check if the row exists in disk store
			if version, found := diskStore.GetVersionFromDisk(rowID); found {
				// Cache the version in memory for future access
				vs.AddVersion(rowID, version)
				return version, true
			}
		}
	}

	return RowVersion{}, false
}

// IterateVisibleVersions iterates through visible versions for the given rowIDs
// and calls the provided callback function for each one, avoiding any map allocation
func (vs *VersionStore) IterateVisibleVersions(rowIDs []int64, txnID int64,
	callback func(rowID int64, version RowVersion) bool) {

	// Check if the version store is closed
	if vs.closed.Load() {
		return
	}

	// Early validation of parameters
	if callback == nil || len(rowIDs) == 0 {
		return
	}

	// Keep track of rowIDs not found in memory to check in disk store
	var notFoundIDs []int64

	// No lock needed with haxmap - it's concurrency-safe
	for _, rowID := range rowIDs {
		// Check again if closed during the iteration
		if vs.closed.Load() {
			return
		}

		versionPtr, exists := vs.versions.Get(rowID)
		if !exists {
			// Keep track of IDs not found in memory for disk lookup
			notFoundIDs = append(notFoundIDs, rowID)
			continue
		}

		// Check visibility
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			// Call the callback with the rowID and a copy of the version
			if !callback(rowID, *versionPtr) {
				// Stop iteration if callback returns false
				return
			}
		}
	}

	// If there are IDs not found in memory and persistence is enabled, check disk store
	if len(notFoundIDs) > 0 && vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Process each rowID not found in memory
			for _, rowID := range notFoundIDs {
				// Check if closed during disk operations
				if vs.closed.Load() {
					return
				}

				// Check if the row exists in disk store
				if version, found := diskStore.GetVersionFromDisk(rowID); found {
					// Cache the version in memory for future access
					vs.AddVersion(rowID, version)

					// Call the callback
					if !callback(rowID, version) {
						// Stop iteration if callback returns false
						return
					}
				}
			}
		}
	}
}

// GetVisibleVersionsByIDs retrieves visible versions for the given rowIDs
// This is an optimized batch version of GetVisibleVersion using fastmap for high performance
func (vs *VersionStore) GetVisibleVersionsByIDs(rowIDs []int64, txnID int64) map[int64]*RowVersion {
	// Check if the version store is closed
	if vs.closed.Load() || vs.versions == nil {
		// Return empty map
		return make(map[int64]*RowVersion)
	}

	// Early validation of parameters
	if len(rowIDs) == 0 {
		return make(map[int64]*RowVersion)
	}

	result := GetVisibleVersionMap(len(rowIDs))

	// Track IDs not found in memory for disk lookup
	var notFoundIDs []int64

	// Process in batches to optimize memory access patterns
	const batchSize = 100
	for i := 0; i < len(rowIDs); i += batchSize {
		// Check again if closed during batch processing
		if vs.closed.Load() {
			break
		}

		end := i + batchSize
		if end > len(rowIDs) {
			end = len(rowIDs)
		}

		// Process this batch
		for j := i; j < end; j++ {
			rowID := rowIDs[j]

			// No lock needed with haxmap - it's concurrency-safe
			versionPtr, exists := vs.versions.Get(rowID)
			if !exists {
				// Track IDs not found for later disk lookup
				notFoundIDs = append(notFoundIDs, rowID)
				continue
			}

			// Check visibility
			if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
				// Add the visible version to result
				result[rowID] = versionPtr
			}
		}
	}

	// If persistence is enabled and we have IDs not found in memory,
	// check the disk store for those IDs
	if len(notFoundIDs) > 0 && vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Check if closed again
			if vs.closed.Load() {
				ReturnVisibleVersionMap(result)
				return make(map[int64]*RowVersion)
			}

			// For optimization, if there are many IDs, use batch retrieval
			if len(notFoundIDs) > 10 {
				// Get versions from disk in batch
				diskVersions := diskStore.GetVersionsBatch(notFoundIDs)

				// Process each disk version
				for rowID, version := range diskVersions {
					if !version.IsDeleted {
						// Cache the version in memory for future access
						vs.AddVersion(rowID, version)

						// Get the cached version pointer from memory to ensure consistency with the pool
						if versionPtr, exists := vs.versions.Get(rowID); exists {
							result[rowID] = versionPtr
						}
					}
				}
			} else {
				// For smaller sets, process individually (which can be faster for few IDs)
				for _, rowID := range notFoundIDs {
					// Check if the row exists in disk store
					if version, found := diskStore.GetVersionFromDisk(rowID); found {
						if !version.IsDeleted {
							// Cache the version in memory for future access
							vs.AddVersion(rowID, version)

							// Get the cached version from memory
							if versionPtr, exists := vs.versions.Get(rowID); exists {
								result[rowID] = versionPtr
							}
						}
					}
				}
			}
		}
	}

	return result
}

// Pool for version maps used in visible version retrieval
var visibleVersionMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[int64]*RowVersion)
	},
}

// GetVisibleVersionMap gets a version map from the pool
func GetVisibleVersionMap(capacity interface{}) map[int64]*RowVersion {
	mapInterface := visibleVersionMapPool.Get()

	// Convert capacity to int if it's uintptr (from haxmap.Len())
	var capInt int
	switch c := capacity.(type) {
	case int:
		capInt = c
	case uintptr:
		capInt = int(c)
	default:
		capInt = 100 // Default capacity
	}

	m, ok := mapInterface.(map[int64]*RowVersion)
	if !ok || m == nil {
		return make(map[int64]*RowVersion, capInt)
	}

	return m
}

// ReturnVisibleVersionMap returns a version map to the pool
func ReturnVisibleVersionMap(m map[int64]*RowVersion) {
	if m == nil {
		return
	}

	clear(m)
	visibleVersionMapPool.Put(m)
}

// We'll manage transaction timestamps directly in the registry without caching

// GetAllVisibleVersions gets all visible versions for a scan operation
// This is an optimized version that reduces allocations and properly respects snapshot isolation
func (vs *VersionStore) GetAllVisibleVersions(txnID int64) map[int64]*RowVersion {
	// Check if the version store is closed or versions is nil
	if vs.closed.Load() || vs.versions == nil {
		// Return empty map
		return make(map[int64]*RowVersion)
	}

	// If we detect we're in the middle of a bulk delete, and in READ COMMITTED mode (the default),
	// we can safely return an empty map since none of the versions matter for the operation
	isBulkDeleteOp := false
	hasSpecificTxnVersions := false

	// Only need to check a small sample to detect bulk operations
	// For single-version approach, we sample a few rows to determine the operation type
	sampleSize := 0
	vs.versions.ForEach(func(k int64, versionPtr *RowVersion) bool {
		// Check if closed during the iteration
		if vs.closed.Load() {
			return false // Stop iteration
		}

		sampleSize++
		if versionPtr.TxnID == txnID {
			hasSpecificTxnVersions = true
			if versionPtr.IsDeleted {
				isBulkDeleteOp = true
			}
		}
		// Return false to break the iteration when we've seen enough samples
		return sampleSize < 5 // Continue only if we haven't reached 5 samples
	})

	// Check if closed after the sampling
	if vs.closed.Load() {
		return make(map[int64]*RowVersion)
	}

	// If this looks like a bulk delete in READ COMMITTED mode, return empty map
	if vs.engine.registry.GetIsolationLevel() == ReadCommitted && isBulkDeleteOp && hasSpecificTxnVersions {
		return map[int64]*RowVersion{}
	}

	// Get preallocated map from pool with a better initial capacity estimate
	estimatedSize := vs.versions.Len() / 2 // Most operations only need half the versions
	result := GetVisibleVersionMap(estimatedSize)

	// Check if the version store is closed
	if vs.closed.Load() {
		// Return empty result if closed
		ReturnVisibleVersionMap(result)
		return make(map[int64]*RowVersion)
	}

	// For bulk operations in READ COMMITTED, optimize the common case
	if vs.engine.registry.GetIsolationLevel() == ReadCommitted {
		vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
			// Check if closed during iteration
			if vs.closed.Load() {
				return false // Stop iteration
			}

			// Skip if it's owned by the current txn (likely being deleted)
			if versionPtr.TxnID == txnID {
				return true // Continue iteration
			}

			// Skip if it's deleted - delete markers aren't visible in queries
			if versionPtr.IsDeleted {
				return true // Continue iteration
			}

			// Skip if it's not committed (only for other txns)
			if !vs.engine.registry.IsDirectlyVisible(versionPtr.TxnID) {
				return true // Continue iteration
			}

			// Visible row - add a copy to result
			result[rowID] = versionPtr
			return true // Continue iteration
		})

		// Check if closed after processing in-memory versions
		if vs.closed.Load() {
			ReturnVisibleVersionMap(result)
			return make(map[int64]*RowVersion)
		}

		// Only check disk if persistence is enabled
		if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
			// Get the disk store for this table
			if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
				// For bulk operations, process the most recent snapshot efficiently using ForEach
				// to avoid unnecessary allocations of the entire map
				newestReader := diskStore.readers[len(diskStore.readers)-1]

				newestReader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
					// Skip deleted rows
					if diskVersion.IsDeleted {
						return true // Continue iteration
					}

					// All rows from disk snapshots have TxnID = -1 and are always visible
					// Cache in memory for future use
					vs.AddVersion(rowID, diskVersion)

					// Get the newly cached version for consistency
					if versionPtr, exists := vs.versions.Get(rowID); exists {
						result[rowID] = versionPtr
					}

					return true // Continue iteration
				})
			}
		}

		return result
	}

	// Fall back to standard visibility rules for SNAPSHOT isolation
	vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
		// Check if closed during iteration
		if vs.closed.Load() {
			return false // Stop iteration
		}

		// No need to track rowIDs separately - we'll check result map directly

		// Skip deleted versions
		if versionPtr.IsDeleted {
			return true // Continue iteration
		}

		// Check for visibility based on isolation level rules
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			result[rowID] = versionPtr
		}
		return true // Continue iteration
	})

	// Final check if closed after in-memory processing
	if vs.closed.Load() {
		ReturnVisibleVersionMap(result)
		return make(map[int64]*RowVersion)
	}

	// For SNAPSHOT isolation, process disk versions
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
			// For snapshot, all rows from disk snapshots have TxnID = -1 and are always visible
			// Start with the most recent snapshot
			newestReader := diskStore.readers[len(diskStore.readers)-1]

			// Use ForEach for memory-efficient iteration without allocating the entire map
			newestReader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
				// Skip deleted versions
				if diskVersion.IsDeleted {
					return true // Continue iteration
				}

				// Cache in memory for future use
				vs.AddVersion(rowID, diskVersion)

				// Get the newly cached version for consistency
				if versionPtr, exists := vs.versions.Get(rowID); exists {
					result[rowID] = versionPtr
				}

				return true // Continue iteration
			})
		}
	}

	return result
}

// TransactionVersionStore holds changes specific to a transaction
type TransactionVersionStore struct {
	localVersions map[int64]RowVersion // RowID -> local version
	parentStore   *VersionStore        // Reference to the shared store
	txnID         int64                // This transaction's ID
	fromPool      bool                 // Whether this object came from the pool
}

// Pool for TransactionVersionStore objects
var transactionVersionStorePool = sync.Pool{
	New: func() interface{} {
		return &TransactionVersionStore{
			localVersions: make(map[int64]RowVersion),
		}
	},
}

// NewTransactionVersionStore creates a transaction-local version store
func NewTransactionVersionStore(
	parentStore *VersionStore,
	txnID int64) *TransactionVersionStore {

	// Get an object from the pool
	tvs := transactionVersionStorePool.Get().(*TransactionVersionStore)

	// Initialize or clear the map
	if tvs.localVersions == nil {
		tvs.localVersions = make(map[int64]RowVersion)
	} else {
		clear(tvs.localVersions)
	}

	// Set the fields
	tvs.parentStore = parentStore
	tvs.txnID = txnID
	tvs.fromPool = true

	return tvs
}

// Put adds or updates a row in the transaction's local store
func (tvs *TransactionVersionStore) Put(rowID int64, data storage.Row, isDelete bool) {
	// Create a row version directly
	rv := RowVersion{
		TxnID:      tvs.txnID,
		IsDeleted:  isDelete,
		Data:       data,
		RowID:      rowID,
		CreateTime: GetFastTimestamp(),
	}

	// Store by value in the local versions map
	tvs.localVersions[rowID] = rv
}

// PutBatch efficiently adds or updates multiple rows with the same operation
// This version is for when all rows have the same data/isDelete values
func (tvs *TransactionVersionStore) PutBatch(rowIDs []int64, data storage.Row, isDelete bool) {
	// Pre-create common field values
	now := GetFastTimestamp()

	// Update for each row ID
	for _, rowID := range rowIDs {
		// Create a row version directly with common fields
		rv := RowVersion{
			TxnID:      tvs.txnID,
			IsDeleted:  isDelete,
			Data:       data,
			RowID:      rowID,
			CreateTime: now,
		}
		tvs.localVersions[rowID] = rv
	}
}

// PutRowsBatch efficiently adds multiple rows with different data values
// This is optimized for batch insert operations
func (tvs *TransactionVersionStore) PutRowsBatch(rowIDs []int64, rows []storage.Row, isDelete bool) {
	// Get a single timestamp for all versions to ensure consistency
	// and avoid multiple system calls
	now := GetFastTimestamp()

	// Pre-allocate a larger map if we're adding a significant number of items
	currentSize := len(tvs.localVersions)
	expectedSize := currentSize + len(rowIDs)
	if expectedSize > 100 && expectedSize > currentSize*2 {
		// Create a new map with more capacity
		newMap := make(map[int64]RowVersion, expectedSize)
		for k, v := range tvs.localVersions {
			newMap[k] = v
		}
		tvs.localVersions = newMap
	}

	// Add all rows with the same timestamp
	for i, rowID := range rowIDs {
		// Create a row version with the data for this row
		rv := RowVersion{
			TxnID:      tvs.txnID,
			IsDeleted:  isDelete,
			Data:       rows[i],
			RowID:      rowID,
			CreateTime: now,
		}
		tvs.localVersions[rowID] = rv
	}
}

// ReleaseTransactionVersionStore returns a TransactionVersionStore to the pool
func ReleaseTransactionVersionStore(tvs *TransactionVersionStore) {
	if tvs == nil || !tvs.fromPool {
		return
	}

	// Clear fields to prevent memory leaks
	clear(tvs.localVersions)
	tvs.parentStore = nil
	tvs.txnID = 0
	tvs.fromPool = false

	// Put back in the pool
	transactionVersionStorePool.Put(tvs)
}

// Rollback aborts the transaction and releases resources
func (tvs *TransactionVersionStore) Rollback() {
	// During rollback, we just need to release resources
	// No need to merge changes to parent store as we're aborting
	if tvs.fromPool {
		// Return this object to the pool
		ReleaseTransactionVersionStore(tvs)
	} else {
		// For backward compatibility with existing code
		tvs.localVersions = nil
	}
}

// HasLocallySeen checks if this rowID has been seen in this transaction
// This is a fast path optimization to avoid the expensive Get operation
func (tvs *TransactionVersionStore) HasLocallySeen(rowID int64) bool {
	_, exists := tvs.localVersions[rowID]
	return exists
}

// Get retrieves a row by its row ID
func (tvs *TransactionVersionStore) Get(rowID int64) (storage.Row, bool) {
	// First check local versions
	if localVersion, exists := tvs.localVersions[rowID]; exists {
		if localVersion.IsDeleted {
			return nil, false
		}
		return localVersion.Data, true
	}

	// If not in local store, check parent store with visibility rules
	if tvs.parentStore != nil {
		if version, exists := tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID); exists {
			if !version.IsDeleted {
				return version.Data, true
			}
			return nil, false
		}

		// Check if engine has persistence enabled and if there's a disk store for this table
		if tvs.parentStore.engine != nil && tvs.parentStore.engine.persistence != nil &&
			tvs.parentStore.engine.persistence.IsEnabled() {

			// Get the disk store for this table
			tableName := tvs.parentStore.tableName
			if diskStore, exists := tvs.parentStore.engine.persistence.diskStores[tableName]; exists {
				// Check if the row exists in the disk store
				if version, found := diskStore.GetVersionFromDisk(rowID); found {
					// All rows from disk snapshots have TxnID = -1 and are always visible
					if !version.IsDeleted {
						// If we found a version on disk, add it to in-memory store for future access
						tvs.parentStore.AddVersion(rowID, version)
						return version.Data, true
					}
					return nil, false
				}
			}
		}
	}

	return nil, false
}

// Pool for row maps to reduce allocations
var rowMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[int64]storage.Row, 16) // Default capacity
	},
}

// GetRowMap gets a map from the pool or creates a new one
func GetRowMap(capacity int) map[int64]storage.Row {
	mapInterface := rowMapPool.Get()

	m, ok := mapInterface.(map[int64]storage.Row)
	if !ok || m == nil {
		return make(map[int64]storage.Row, capacity)
	}

	clear(m)

	return m
}

// ReturnRowMap returns a map to the pool
func PutRowMap(m map[int64]storage.Row) {
	if m == nil {
		return
	}

	clear(m)
	rowMapPool.Put(m)
}

// GetAllVisibleRows retrieves all rows visible to this transaction
// This version implements zero-copy semantics where possible to reduce allocations
// and uses optimized batch processing for disk data with caching
func (tvs *TransactionVersionStore) GetAllVisibleRows() map[int64]storage.Row {
	// Reuse maps from the pool - first estimate capacity we need
	capacity := 100 // Default capacity

	// If possible, get a better estimate of size
	if tvs.parentStore != nil && tvs.parentStore.versions.Len() > 0 {
		capacity = int(tvs.parentStore.versions.Len())
	}

	// Get a preallocated map from the pool
	result := GetRowMap(capacity)

	// Get globally visible versions directly from the parent store
	if tvs.parentStore != nil {
		vs := tvs.parentStore
		txnID := tvs.txnID
		registry := tvs.parentStore.engine.registry

		// Process in-memory versions first
		// Fast path for small-medium tables, direct iteration is more efficient
		if txnID > 0 && registry != nil && vs.versions.Len() < 10000 {
			// Create a simple callback function that avoids closure allocation
			process := func(rowID int64, versionPtr *RowVersion) bool {
				// Skip deleted versions
				if versionPtr.IsDeleted {
					return true // Continue iteration
				}

				// Fast visibility check for common patterns
				// Most common case: our transaction seeing committed data
				if versionPtr.TxnID != txnID && registry.IsDirectlyVisible(versionPtr.TxnID) {
					result[rowID] = versionPtr.Data
				} else if versionPtr.TxnID == txnID {
					// Direct access to our own transaction's data
					result[rowID] = versionPtr.Data
				} else if registry.IsVisible(versionPtr.TxnID, txnID) {
					// Full visibility check for complex cases
					result[rowID] = versionPtr.Data
				}
				return true // Continue iteration
			}

			// Use the non-closure function to avoid allocations
			vs.versions.ForEach(process)
		} else {
			// Legacy path for larger tables
			vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
				// Skip deleted versions
				if versionPtr.IsDeleted {
					return true // Continue iteration
				}

				// Check visibility
				if registry.IsVisible(versionPtr.TxnID, txnID) {
					// Add directly to result
					result[rowID] = versionPtr.Data
				}
				return true // Continue iteration
			})
		}

		// Check for disk-stored rows if persistence is enabled
		if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
			// Get the disk store for this table
			if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
				reader := diskStore.readers[len(diskStore.readers)-1]

				reader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
					// Skip deleted rows
					if diskVersion.IsDeleted {
						return true // Continue iteration
					}

					// All rows from disk snapshots have TxnID = -1 and are always visible
					result[rowID] = diskVersion.Data

					// Cache in memory for future use
					vs.AddVersion(rowID, diskVersion)

					return true // Continue iteration
				})
			}
		}
	}

	// Process local versions (these take precedence)
	for rowID, version := range tvs.localVersions {
		if version.IsDeleted {
			// If deleted locally, remove from result
			delete(result, rowID)
		} else {
			// Local versions must be copied since they may be modified during transaction
			result[rowID] = version.Data
		}
	}

	return result
}

// Commit merges local changes into the parent version store
func (tvs *TransactionVersionStore) Commit() {
	// Add all local versions to the parent store
	if tvs.parentStore != nil {
		for rowID, version := range tvs.localVersions {
			tvs.parentStore.AddVersion(rowID, version)
		}
	}

	// If from pool, return it after commit
	if tvs.fromPool {
		ReleaseTransactionVersionStore(tvs)
	} else {
		// For backward compatibility with existing code
		tvs.localVersions = nil
	}
}

// CreateColumnarIndex creates a columnar index for a specific column
func (vs *VersionStore) CreateColumnarIndex(tableName string, columnName string, columnID int,
	dataType storage.DataType, isUnique bool, customName string) (storage.Index, error) {

	// Check if the version store is closed using atomic operation
	if vs.closed.Load() {
		return nil, errors.New("version store is closed")
	}

	// First check with a read lock to see if the index already exists
	vs.columnarMutex.RLock()
	_, exists := vs.columnarIndexes[columnName]
	vs.columnarMutex.RUnlock()

	if exists {
		return nil, fmt.Errorf("columnar index for column %s already exists", columnName)
	}

	// Create index name, using customName if provided
	indexName := customName
	if indexName == "" {
		// Generate default name if custom name is not provided
		if isUnique {
			indexName = fmt.Sprintf("unique_columnar_%s_%s", tableName, columnName)
		} else {
			indexName = fmt.Sprintf("columnar_%s_%s", tableName, columnName)
		}
	}

	// Use the btree implementation with the isUnique parameter
	index := NewColumnarIndex(indexName, tableName, columnName, columnID, dataType, vs, isUnique)

	// Build the index from existing data
	// This is done outside of lock to avoid holding the lock during expensive operations
	err := index.Build()
	if err != nil {
		// Close the index to clean up any resources
		index.Close()
		return nil, err
	}

	// Check again if the store was closed during our potentially long build operation
	if vs.closed.Load() {
		// Close the index to clean up any resources
		index.Close()
		return nil, errors.New("version store is closed")
	}

	// Now acquire the lock to update the map
	vs.columnarMutex.Lock()
	defer vs.columnarMutex.Unlock()

	// Check again if the index already exists
	// Someone else might have created it while we were building
	if _, exists := vs.columnarIndexes[columnName]; exists {
		// Close the index to clean up any resources, since we won't be using it
		index.Close()
		return nil, fmt.Errorf("columnar index for column %s already exists", columnName)
	}

	// One final check if the version store was closed while we were waiting for the lock
	if vs.closed.Load() {
		// Close the index to clean up any resources
		index.Close()
		return nil, errors.New("version store is closed")
	}

	// Store in the map
	vs.columnarIndexes[columnName] = index

	return index, nil
}

// GetColumnarIndex retrieves a columnar index by column name
func (vs *VersionStore) GetColumnarIndex(columnName string) (storage.Index, error) {
	// Check if the version store is closed using atomic operation
	if vs.closed.Load() {
		return nil, errors.New("version store is closed")
	}

	// Acquire read lock to access the indexes map
	vs.columnarMutex.RLock()
	defer vs.columnarMutex.RUnlock()

	// Check if the index exists
	index, exists := vs.columnarIndexes[columnName]
	if !exists {
		return nil, fmt.Errorf("columnar index for column %s not found", columnName)
	}

	return index, nil
}

// Close releases resources associated with this version store
func (vs *VersionStore) Close() error {
	// Use atomic CompareAndSwap to ensure only one goroutine will do the actual closing
	// This is a more efficient replacement for the mutex-based approach
	if !vs.closed.CompareAndSwap(false, true) {
		// If already closed or another goroutine is closing it, return early
		return nil
	}

	// At this point we're the only goroutine that will execute the cleanup code
	// because we successfully changed the state from false to true

	// Clear all columnar indexes - still need a lock for map access
	vs.columnarMutex.Lock()
	for name, index := range vs.columnarIndexes {
		if index != nil {
			// Call any cleanup needed for the index
			if closeableIndex, ok := index.(interface{ Close() error }); ok {
				_ = closeableIndex.Close() // Ignore errors during cleanup
			}
		}
		delete(vs.columnarIndexes, name)
	}
	vs.columnarMutex.Unlock()

	// Clear all versions to release memory
	vs.versionsMu.Lock()
	vs.versions = nil
	vs.versionsMu.Unlock()

	// The closed state is already set to true from the CompareAndSwap above
	return nil
}

// IndexExists checks if an index exists for this table
func (vs *VersionStore) IndexExists(indexName string) bool {
	// If the store is closed, no indexes exist
	if vs.closed.Load() {
		return false
	}

	vs.columnarMutex.RLock()
	defer vs.columnarMutex.RUnlock()

	// First, check if the name directly matches a column with an index
	if _, exists := vs.columnarIndexes[indexName]; exists {
		return true
	}

	// Second, check if any index has the given name
	for _, index := range vs.columnarIndexes {
		if index.Name() == indexName {
			return true
		}
	}

	return false
}

// ListIndexes returns all indexes for this table
// The returned map has the index name as the key and the column name as the value
// This ensures that custom index names are preserved and can be used for lookup
func (vs *VersionStore) ListIndexes() map[string]string {
	// If the store is closed, return empty list
	if vs.closed.Load() {
		return map[string]string{}
	}

	vs.columnarMutex.RLock()
	defer vs.columnarMutex.RUnlock()

	indexes := make(map[string]string, len(vs.columnarIndexes))
	for colName, index := range vs.columnarIndexes {
		// Use the actual index name as the key, not the column name
		// This ensures custom index names are preserved and can be used as keys
		indexName := index.Name()
		indexes[indexName] = colName
	}

	return indexes
}

// GetTableSchema returns the schema for this table
func (vs *VersionStore) GetTableSchema() (storage.Schema, error) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return storage.Schema{}, errors.New("version store is closed")
	}

	// Use the engine reference that was provided during initialization
	if vs.engine == nil {
		return storage.Schema{}, errors.New("engine not available")
	}

	// Get the schema from the engine
	return vs.engine.GetTableSchema(vs.tableName)
}

// AddIndex adds an index to the version store
func (vs *VersionStore) AddIndex(index storage.Index) error {
	// Check if the version store is closed
	if vs.closed.Load() {
		return errors.New("version store is closed")
	}

	// Check if index is nil
	if index == nil {
		return errors.New("index cannot be nil")
	}

	// Get the index name
	indexName := index.Name()

	vs.columnarMutex.Lock()
	defer vs.columnarMutex.Unlock()

	// Check if an index with this name already exists
	for _, existing := range vs.columnarIndexes {
		if existing.Name() == indexName {
			return fmt.Errorf("index %s already exists", indexName)
		}
	}

	// Get the column names - columnar indexes only have one column
	columnNames := index.ColumnNames()
	if len(columnNames) != 1 {
		return fmt.Errorf("columnar index must have exactly one column, got %d", len(columnNames))
	}

	// Store the index by column name for fast lookups
	columnName := columnNames[0]
	vs.columnarIndexes[columnName] = index

	return nil
}

// RemoveIndex removes an index from the version store
func (vs *VersionStore) RemoveIndex(indexName string) error {
	// Check if the version store is closed
	if vs.closed.Load() {
		return errors.New("version store is closed")
	}

	vs.columnarMutex.Lock()
	defer vs.columnarMutex.Unlock()

	// Find the index by name
	for columnName, index := range vs.columnarIndexes {
		if index.Name() == indexName {
			// Close the index if it has a Close method
			if closeableIndex, ok := index.(interface{ Close() error }); ok {
				_ = closeableIndex.Close() // Ignore errors during cleanup
			}

			// Remove from the map
			delete(vs.columnarIndexes, columnName)

			return nil
		}
	}

	return fmt.Errorf("index %s not found", indexName)
}

// UpdateColumnarIndexes updates all columnar indexes with a new row version
func (vs *VersionStore) UpdateColumnarIndexes(rowID int64, version RowVersion) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return // Skip index updates if closed
	}

	vs.columnarMutex.RLock()
	defer vs.columnarMutex.RUnlock()

	// If there are no columnar indexes, we can skip this
	if len(vs.columnarIndexes) == 0 {
		return
	}

	// If the row is deleted, remove from all indexes
	if version.IsDeleted || version.Data == nil {
		for _, index := range vs.columnarIndexes {
			// Find column ID for this index
			columnID := index.ColumnID()

			// If the column exists in the row, remove its old value
			if columnID < len(version.Data) {
				index.Remove(version.Data[columnID], rowID, 0)
			}
		}
		return
	}

	// For non-deleted rows, update all relevant indexes
	for _, index := range vs.columnarIndexes {
		// Find column ID for this index
		columnID := index.ColumnID()

		// If the column exists in the row, add its new value
		if columnID < len(version.Data) {
			index.Add(version.Data[columnID], rowID, 0)
		} else {
			// Column doesn't exist, treat as NULL
			index.Add(nil, rowID, 0)
		}
	}
}
