// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/semihalev/stoolap/internal/btree"
	"github.com/semihalev/stoolap/internal/storage"
	"github.com/semihalev/stoolap/internal/storage/binser"
)

// Constants for file layout
const (
	FileHeaderSize    = 16                 // Magic bytes + version
	IndexEntrySize    = 16                 // RowID (8 bytes) + Offset (8 bytes)
	FooterSize        = 48                 // IndexOffset (8) + IndexSize (8) + RowCount (8) + TxnIDsOffset (8) + TxnIDsCount (8) + Magic (8)
	MagicBytes        = 0x5354534456534844 // "STSVSHD" (SToolaP VerSion Store HarD disk)
	FileFormatVersion = 1
	DefaultBatchSize  = 1000
	DefaultBlockSize  = 64 * 1024 // 64KB
)

// Constants for snapshot operation
const (
	// Default values
	DefaultKeepSnapshots = 5

	// Schema format
	SchemaVersion    = 1
	SchemaHeaderSize = 16

	// Default timeout values
	DefaultSnapshotIOTimeout   = 10 * time.Second
	DefaultSnapshotLockTimeout = 2 * time.Second
)

// FileHeader contains the metadata at the beginning of a disk version file
type FileHeader struct {
	Magic   uint64
	Version uint32
	Flags   uint32
}

// Footer contains metadata at the end of a disk version file
type Footer struct {
	IndexOffset  uint64
	IndexSize    uint64
	RowCount     uint64
	TxnIDsOffset uint64 // Offset where committed transaction IDs are stored
	TxnIDsCount  uint64 // Number of committed transaction IDs
	Magic        uint64
}

// DiskVersionStore manages the on-disk extension of the version store
type DiskVersionStore struct {
	mu           sync.RWMutex
	baseDir      string
	tableName    string
	schemaHash   uint64 // For schema verification
	readers      []*DiskReader
	versionStore *VersionStore // Reference to in-memory version store
}

// NewDiskVersionStore creates a new disk version store for a table
// If the schema is provided, it will be used instead of querying the version store
func NewDiskVersionStore(baseDir, tableName string, versionStore *VersionStore, providedSchema ...storage.Schema) (*DiskVersionStore, error) {
	storeDir := filepath.Join(baseDir, tableName)
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create disk version store directory: %w", err)
	}

	var schema storage.Schema
	var err error

	// Use provided schema if available (to avoid lock contention)
	if len(providedSchema) > 0 {
		schema = providedSchema[0]
	} else {
		// Otherwise get it from the version store
		schema, err = versionStore.GetTableSchema()
		if err != nil {
			return nil, fmt.Errorf("failed to get table schema: %w", err)
		}
	}

	dvs := &DiskVersionStore{
		baseDir:      baseDir,
		tableName:    tableName,
		schemaHash:   schemaHash(schema),
		versionStore: versionStore,
	}

	return dvs, nil
}

// CreateSnapshot creates a new snapshot of the table's data
func (dvs *DiskVersionStore) CreateSnapshot() error {
	timestamp := time.Now().Format("20060102-150405.000")
	filePath := filepath.Join(dvs.baseDir, dvs.tableName, fmt.Sprintf("snapshot-%s.bin", timestamp))

	// Create a new appender
	appender, err := NewDiskAppender(filePath)
	if err != nil {
		return fmt.Errorf("failed to create disk appender: %w", err)
	}
	defer appender.Close()

	// Get schema for the table
	schema, err := dvs.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	// Write schema to file
	if err := appender.WriteSchema(&schema); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Process data in batches without locking the whole table
	batch := make([]RowVersion, 0, DefaultBatchSize)

	// Use ForEach to avoid locking the whole table
	dvs.versionStore.versions.ForEach(func(rowID int64, version *RowVersion) bool {
		// Copy the version to avoid concurrent modification issues
		versionCopy := *version

		// Add to batch
		batch = append(batch, versionCopy)

		// Process batch when full
		if len(batch) >= DefaultBatchSize {
			if err := appender.AppendBatch(batch); err != nil {
				fmt.Printf("Error: appending batch: %v\n", err)
				return false // Stop iteration on error
			}
			batch = batch[:0] // Reset batch
		}

		return true
	})

	// Process any remaining rows
	if len(batch) > 0 {
		if err := appender.AppendBatch(batch); err != nil {
			return fmt.Errorf("failed to append final batch: %w", err)
		}
	}

	// Finalize the snapshot file
	if err := appender.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	// Create metadata file
	metaPath := strings.TrimSuffix(filePath, ".bin") + ".meta"
	if err := dvs.writeMetadata(metaPath, &schema); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Add reader for the new snapshot
	reader, err := NewDiskReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to create disk reader: %w", err)
	}

	// Add to readers list
	dvs.mu.Lock()
	dvs.readers = append(dvs.readers, reader)
	dvs.mu.Unlock()

	// Access the engine persistence to create a checkpoint and truncate WAL
	// Only create a checkpoint if we have access to the persistence and WAL managers
	if dvs.versionStore.engine != nil &&
		dvs.versionStore.engine.persistence != nil &&
		dvs.versionStore.engine.persistence.wal != nil {

		// Get the current LSN from the WAL
		currentLSN := dvs.versionStore.engine.persistence.wal.currentLSN.Load()

		// Create the checkpoint with the current LSN
		dvs.versionStore.engine.persistence.wal.createConsistentCheckpoint(currentLSN, true)

		// Update persistence metadata with the snapshot information
		dvs.versionStore.engine.persistence.meta.lastWALLSN.Store(currentLSN)
		dvs.versionStore.engine.persistence.meta.lastSnapshotLSN.Store(currentLSN)
		dvs.versionStore.engine.persistence.meta.lastSnapshotTimeNano.Store(time.Now().UnixNano())

		// Truncate the WAL file using the checkpoint LSN
		// This removes entries that are already captured in the snapshot
		err := dvs.versionStore.engine.persistence.wal.TruncateWAL(currentLSN)
		if err != nil {
			fmt.Printf("Warning: Failed to truncate WAL after checkpoint: %v\n", err)
			// Continue despite error since this is not critical
		}
	}

	return nil
}

// writeMetadata writes metadata about the snapshot
func (dvs *DiskVersionStore) writeMetadata(path string, schema *storage.Schema) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := binser.NewWriter()
	defer writer.Release()

	// Write format version
	writer.WriteUint32(FileFormatVersion)

	// Write timestamp
	writer.WriteInt64(time.Now().UnixNano())

	// Write schema hash
	writer.WriteUint64(dvs.schemaHash)

	// Write schema itself (for verification)
	schemaBytes, err := serializeSchema(schema)
	if err != nil {
		return err
	}
	writer.WriteBytes(schemaBytes)

	// Save columnar indexes metadata
	var indexDataList [][]byte
	dvs.versionStore.columnarMutex.RLock()
	// Write index count
	writer.WriteUint16(uint16(len(dvs.versionStore.columnarIndexes)))

	// Collect and serialize index data
	for _, index := range dvs.versionStore.columnarIndexes {
		if colIndex, ok := index.(*ColumnarIndex); ok {
			indexData, err := SerializeIndexMetadata(colIndex)
			if err != nil {
				dvs.versionStore.columnarMutex.RUnlock()
				return fmt.Errorf("failed to serialize index metadata: %w", err)
			}
			indexDataList = append(indexDataList, indexData)
		}
	}
	dvs.versionStore.columnarMutex.RUnlock()

	// Write each index's metadata
	for _, indexData := range indexDataList {
		writer.WriteUint32(uint32(len(indexData)))
		writer.WriteBytes(indexData)
	}

	_, err = file.Write(writer.Bytes())
	if err != nil {
		return err
	}

	return file.Sync()
}

// LoadSnapshots loads all snapshots for the table
func (dvs *DiskVersionStore) LoadSnapshots() error {
	tableDir := filepath.Join(dvs.baseDir, dvs.tableName)

	// Check if directory exists
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return nil // No snapshots yet
	}

	// Read directory
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshots directory: %w", err)
	}

	var snapshotFiles []string
	var metadataFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			if strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".bin") {
				snapshotFiles = append(snapshotFiles, filepath.Join(tableDir, entry.Name()))
			} else if strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".meta") {
				metadataFiles = append(metadataFiles, filepath.Join(tableDir, entry.Name()))
			}
		}
	}

	// Sort by name (which includes timestamp)
	sort.Strings(snapshotFiles)
	sort.Strings(metadataFiles)

	// Get transaction registry from version store's engine
	registry := dvs.versionStore.engine.GetRegistry()
	if registry == nil {
		return fmt.Errorf("transaction registry not found")
	}

	// First load metadata files to get index information
	// Since they should be paired with snapshot files, we'll only need the newest one
	if len(metadataFiles) > 0 {
		// Get the most recent metadata file
		metaFile := metadataFiles[len(metadataFiles)-1]
		err := dvs.loadMetadataFile(metaFile)
		if err != nil {
			fmt.Printf("Warning: Failed to load metadata %s: %v\n", metaFile, err)
		}
	}

	// Load each snapshot file
	for _, file := range snapshotFiles {
		reader, err := NewDiskReader(file)
		if err != nil {
			fmt.Printf("Warning: Failed to load snapshot %s: %v\n", file, err)
			continue
		}

		// Get committed transaction IDs from the snapshot and register them
		// This ensures proper visibility for versions stored in snapshots
		committedTxns := reader.GetCommittedTransactions()
		for txnID, commitTS := range committedTxns {
			// Register each transaction with the transaction registry
			registry.RecoverCommittedTransaction(txnID, commitTS)
		}

		dvs.mu.Lock()
		dvs.readers = append(dvs.readers, reader)
		dvs.mu.Unlock()
	}

	return nil
}

// loadMetadataFile loads and processes a metadata file, including recreating columnar indexes
func (dvs *DiskVersionStore) loadMetadataFile(path string) error {
	// Open the metadata file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read the entire file
	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	reader := binser.NewReader(data)

	// Read format version
	version, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != FileFormatVersion {
		return fmt.Errorf("incompatible file format version: %d", version)
	}

	// Read timestamp
	_, err = reader.ReadInt64() // Timestamp, not used here
	if err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read schema hash
	schemaHash, err := reader.ReadUint64()
	if err != nil {
		return fmt.Errorf("failed to read schema hash: %w", err)
	}
	if schemaHash != dvs.schemaHash {
		return fmt.Errorf("schema hash mismatch: %d != %d", schemaHash, dvs.schemaHash)
	}

	// Skip schema bytes
	schemaBytes, err := reader.ReadBytes()
	if err != nil {
		return fmt.Errorf("failed to read schema bytes: %w", err)
	}
	if len(schemaBytes) == 0 {
		return fmt.Errorf("empty schema bytes")
	}

	// Read index count
	indexCount, err := reader.ReadUint16()
	if err != nil {
		return fmt.Errorf("failed to read index count: %w", err)
	}

	// Process each index
	var createdIndexes []*ColumnarIndex

	for i := uint16(0); i < indexCount; i++ {
		// Read index data length
		indexLen, err := reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read index data length: %w", err)
		}

		// Read index data
		indexData, err := reader.ReadBytes()
		if err != nil {
			return fmt.Errorf("failed to read index data: %w", err)
		}
		if uint32(len(indexData)) != indexLen {
			return fmt.Errorf("index data length mismatch: %d != %d", len(indexData), indexLen)
		}

		// Deserialize index metadata
		indexMeta, err := DeserializeIndexMetadata(indexData)
		if err != nil {
			return fmt.Errorf("failed to deserialize index metadata: %w", err)
		}

		// Check if this index already exists
		exists := false
		dvs.versionStore.columnarMutex.RLock()
		for name := range dvs.versionStore.columnarIndexes {
			if name == indexMeta.ColumnName {
				exists = true
				break
			}
		}
		dvs.versionStore.columnarMutex.RUnlock()

		// Skip if the index already exists
		if exists {
			continue
		}

		// Create new columnar index
		index, err := dvs.versionStore.CreateColumnarIndex(
			indexMeta.TableName,
			indexMeta.ColumnName,
			indexMeta.ColumnID,
			indexMeta.DataType,
			indexMeta.IsUnique,
		)
		if err != nil {
			return fmt.Errorf("failed to create columnar index: %w", err)
		}

		// If it's a time index, configure time bucketing
		if colIndex, ok := index.(*ColumnarIndex); ok && indexMeta.EnableTimeBucketing {
			colIndex.EnableTimeBucketing(indexMeta.TimeGranularity)
		}

		// Add to list of created indexes
		if colIndex, ok := index.(*ColumnarIndex); ok {
			createdIndexes = append(createdIndexes, colIndex)
		}
	}

	// Build indexes using optimized Build method
	if len(createdIndexes) > 0 {
		// Use the index's Build method which is optimized for bulk loading
		for _, colIndex := range createdIndexes {
			// Build the index from the version store
			// This automatically scans rows from both memory and disk
			if err := colIndex.Build(); err != nil {
				// Log error but continue with other indexes
				fmt.Printf("Warning: Failed to build index %s: %v\n", colIndex.name, err)
			}
		}
	}

	return nil
}

// QuickCheckRowExists checks if the rowID exists in any disk snapshot without reading the full row
// This is a fast path for existence checks, used by HasRowIDWithQuickCheck
func (dvs *DiskVersionStore) QuickCheckRowExists(rowID int64) bool {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	// Check each reader from newest to oldest
	for i := len(dvs.readers) - 1; i >= 0; i-- {
		// Check if the rowID exists in the index without reading the row
		if dvs.readers[i].index != nil {
			_, found := dvs.readers[i].index.Search(rowID)
			if found {
				return true
			}
		}
	}

	return false
}

// GetVersionFromDisk tries to find a row version on disk
func (dvs *DiskVersionStore) GetVersionFromDisk(rowID int64) (RowVersion, bool) {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	// Check each reader from newest to oldest
	for i := len(dvs.readers) - 1; i >= 0; i-- {
		version, found := dvs.readers[i].GetRow(rowID)
		if found {
			return version, true
		}
	}

	return RowVersion{}, false
}

// GetVersionsInRange retrieves all row versions with rowIDs in the given range
func (dvs *DiskVersionStore) GetVersionsInRange(minRowID, maxRowID int64) []RowVersion {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	if len(dvs.readers) == 0 {
		return nil
	}

	// Use the most recent reader for range query
	reader := dvs.readers[len(dvs.readers)-1]

	// Get the B-Tree index entries in the range
	values := reader.index.RangeSearch(minRowID, maxRowID)
	if len(values) == 0 {
		return nil
	}

	// Allocate results array
	results := make([]RowVersion, 0, len(values))

	// Read row versions from disk
	for _, pair := range values {
		// Read length prefix using reader's pre-allocated buffer
		if _, err := reader.file.ReadAt(reader.lenBuffer, pair); err != nil {
			continue
		}
		rowLen := binary.LittleEndian.Uint32(reader.lenBuffer)

		// Read row data
		rowBytes := make([]byte, rowLen)
		if _, err := reader.file.ReadAt(rowBytes, pair+4); err != nil {
			continue
		}

		// Deserialize row version
		version, err := deserializeRowVersion(rowBytes)
		if err != nil {
			continue
		}

		results = append(results, version)
	}

	return results
}

// GetVersionsBatch retrieves multiple row versions by their IDs
func (dvs *DiskVersionStore) GetVersionsBatch(rowIDs []int64) map[int64]RowVersion {
	if len(rowIDs) == 0 {
		return nil
	}

	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	if len(dvs.readers) == 0 {
		return nil
	}

	result := make(map[int64]RowVersion, len(rowIDs))

	// Start with the newest reader
	for i := len(dvs.readers) - 1; i >= 0; i-- {
		reader := dvs.readers[i]

		// Process only rowIDs we haven't found yet
		var remaining []int64
		for _, id := range rowIDs {
			if _, found := result[id]; !found {
				remaining = append(remaining, id)
			}
		}

		// If we found all rows, we're done
		if len(remaining) == 0 {
			break
		}

		// Get rows from this reader
		versions := reader.GetRowBatch(remaining)

		// Add found versions to the result
		for id, version := range versions {
			result[id] = version
		}
	}

	return result
}

// Close closes the disk version store and all its readers
func (dvs *DiskVersionStore) Close() error {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	var lastErr error
	for _, reader := range dvs.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}

	// Clear readers to release memory
	dvs.readers = nil

	return lastErr
}

// DiskAppender handles writing row versions to disk
type DiskAppender struct {
	file            *os.File
	writer          *bufio.Writer
	indexBuffer     *bytes.Buffer
	dataOffset      uint64
	rowCount        uint64
	rowIDIndex      map[int64]uint64 // Maps rowIDs to offsets
	committedTxnIDs map[int64]int64  // Maps committed TxnIDs to timestamps
}

// NewDiskAppender creates a new disk appender
func NewDiskAppender(filePath string) (*DiskAppender, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Initialize with 16 byte header
	header := FileHeader{
		Magic:   MagicBytes,
		Version: FileFormatVersion,
		Flags:   0,
	}

	headerBytes := make([]byte, FileHeaderSize)
	binary.LittleEndian.PutUint64(headerBytes[0:8], header.Magic)
	binary.LittleEndian.PutUint32(headerBytes[8:12], header.Version)
	binary.LittleEndian.PutUint32(headerBytes[12:16], header.Flags)

	writer := bufio.NewWriterSize(file, DefaultBlockSize)
	if _, err := writer.Write(headerBytes); err != nil {
		file.Close()
		return nil, err
	}

	return &DiskAppender{
		file:            file,
		writer:          writer,
		indexBuffer:     bytes.NewBuffer(make([]byte, 0, 1024*1024)), // 1MB initial capacity
		dataOffset:      FileHeaderSize,                              // Start after header
		rowIDIndex:      make(map[int64]uint64),
		committedTxnIDs: make(map[int64]int64), // Initialize map of committed TxnIDs
	}, nil
}

// Close closes the appender and its file
func (a *DiskAppender) Close() error {
	if a.file != nil {
		if a.writer != nil {
			a.writer.Flush()
		}
		return a.file.Close()
	}
	return nil
}

// WriteSchema writes the table schema to the file
func (a *DiskAppender) WriteSchema(schema *storage.Schema) error {
	schemaBytes, err := serializeSchema(schema)
	if err != nil {
		return err
	}

	// Write length of schema data (uint32)
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(schemaBytes)))
	if _, err := a.writer.Write(lenBytes); err != nil {
		return err
	}

	// Write schema data
	if _, err := a.writer.Write(schemaBytes); err != nil {
		return err
	}

	// Update offset
	a.dataOffset += 4 + uint64(len(schemaBytes))

	return nil
}

// AppendRow appends a single row version to the file
func (a *DiskAppender) AppendRow(version RowVersion) error {
	// Serialize row
	rowBytes, err := serializeRowVersion(version)
	if err != nil {
		return err
	}

	// Check if this rowID already exists
	if _, exists := a.rowIDIndex[version.RowID]; exists {
		return fmt.Errorf("duplicate rowID: %d", version.RowID)
	}

	// Store the offset in our index
	a.rowIDIndex[version.RowID] = a.dataOffset

	// Track committed transaction ID and timestamp
	// If this version is in the snapshot, its transaction must have been committed
	// Store the CreateTime as the approximate commit timestamp
	a.committedTxnIDs[version.TxnID] = version.CreateTime

	// Write row length (uint32)
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(rowBytes)))
	if _, err := a.writer.Write(lenBytes); err != nil {
		return err
	}

	// Write row data
	if _, err := a.writer.Write(rowBytes); err != nil {
		return err
	}

	// Update offset and counter
	a.dataOffset += 4 + uint64(len(rowBytes))
	a.rowCount++

	return nil
}

// AppendBatch appends a batch of row versions to the file
func (a *DiskAppender) AppendBatch(versions []RowVersion) error {
	for _, version := range versions {
		if err := a.AppendRow(version); err != nil {
			return err
		}
	}
	return nil
}

// Finalize completes the file by writing the index and footer
func (a *DiskAppender) Finalize() error {
	// Flush any pending data
	if err := a.writer.Flush(); err != nil {
		return err
	}

	// Build index - sort by rowID for binary search
	var sortedKeys []int64
	for rowID := range a.rowIDIndex {
		sortedKeys = append(sortedKeys, rowID)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return sortedKeys[i] < sortedKeys[j]
	})

	// Write each index entry
	indexOffset := a.dataOffset
	for _, rowID := range sortedKeys {
		offset := a.rowIDIndex[rowID]

		// Write rowID (int64)
		rowIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(rowIDBytes, uint64(rowID))
		a.indexBuffer.Write(rowIDBytes)

		// Write offset (uint64)
		offsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBytes, offset)
		a.indexBuffer.Write(offsetBytes)
	}

	// Write index to file
	indexBytes := a.indexBuffer.Bytes()
	if _, err := a.file.WriteAt(indexBytes, int64(indexOffset)); err != nil {
		return err
	}

	// Prepare TxnIDs data - write transaction IDs and their commit timestamps
	txnIDsOffset := indexOffset + uint64(len(indexBytes))
	txnIDsBuffer := bytes.NewBuffer(make([]byte, 0, len(a.committedTxnIDs)*16)) // Each entry is 16 bytes (TxnID + Timestamp)

	for txnID, timestamp := range a.committedTxnIDs {
		// Write TxnID (int64)
		txnIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(txnIDBytes, uint64(txnID))
		txnIDsBuffer.Write(txnIDBytes)

		// Write commit timestamp (int64)
		timestampBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestampBytes, uint64(timestamp))
		txnIDsBuffer.Write(timestampBytes)
	}

	// Write transaction IDs to file
	txnIDsBytes := txnIDsBuffer.Bytes()
	if _, err := a.file.WriteAt(txnIDsBytes, int64(txnIDsOffset)); err != nil {
		return err
	}

	// Prepare footer
	footer := Footer{
		IndexOffset:  indexOffset,
		IndexSize:    uint64(len(indexBytes)),
		RowCount:     a.rowCount,
		TxnIDsOffset: txnIDsOffset,
		TxnIDsCount:  uint64(len(a.committedTxnIDs)),
		Magic:        MagicBytes,
	}

	// Write footer
	footerOffset := txnIDsOffset + uint64(len(txnIDsBytes))
	footerBytes := make([]byte, 48) // Updated footer size with TxnIDs fields
	binary.LittleEndian.PutUint64(footerBytes[0:8], footer.IndexOffset)
	binary.LittleEndian.PutUint64(footerBytes[8:16], footer.IndexSize)
	binary.LittleEndian.PutUint64(footerBytes[16:24], footer.RowCount)
	binary.LittleEndian.PutUint64(footerBytes[24:32], footer.TxnIDsOffset)
	binary.LittleEndian.PutUint64(footerBytes[32:40], footer.TxnIDsCount)
	binary.LittleEndian.PutUint64(footerBytes[40:48], footer.Magic)

	if _, err := a.file.WriteAt(footerBytes, int64(footerOffset)); err != nil {
		return err
	}

	// Force sync to ensure data is on disk
	return a.file.Sync()
}

// DiskReader handles reading row versions from disk
type DiskReader struct {
	mu       sync.RWMutex
	file     *os.File
	filePath string

	// File components
	fileSize int64
	header   FileHeader
	footer   Footer
	schema   *storage.Schema

	// Row access using optimized B-Tree index
	index *btree.Int64BTree[int64] // B-Tree index for fast lookups

	// Committed transactions tracking
	committedTxnIDs map[int64]int64 // Maps committed TxnIDs to timestamps

	// Read buffers to reduce allocations in hot paths
	lenBuffer []byte // Buffer for reading length prefix
}

// NewDiskReader creates a new disk reader
func NewDiskReader(filePath string) (*DiskReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	reader := &DiskReader{
		file:            file,
		filePath:        filePath,
		fileSize:        stat.Size(),
		lenBuffer:       make([]byte, 4),       // Pre-allocate buffer for length prefix
		committedTxnIDs: make(map[int64]int64), // Initialize transaction map
	}

	// Read header
	headerBytes := make([]byte, FileHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		file.Close()
		return nil, err
	}

	reader.header.Magic = binary.LittleEndian.Uint64(headerBytes[0:8])
	reader.header.Version = binary.LittleEndian.Uint32(headerBytes[8:12])
	reader.header.Flags = binary.LittleEndian.Uint32(headerBytes[12:16])

	// Validate magic bytes
	if reader.header.Magic != MagicBytes {
		file.Close()
		return nil, fmt.Errorf("invalid file format: magic mismatch")
	}

	// Read footer
	footerBytes := make([]byte, FooterSize)
	if _, err := file.ReadAt(footerBytes, stat.Size()-FooterSize); err != nil {
		file.Close()
		return nil, err
	}

	reader.footer.IndexOffset = binary.LittleEndian.Uint64(footerBytes[0:8])
	reader.footer.IndexSize = binary.LittleEndian.Uint64(footerBytes[8:16])
	reader.footer.RowCount = binary.LittleEndian.Uint64(footerBytes[16:24])
	reader.footer.TxnIDsOffset = binary.LittleEndian.Uint64(footerBytes[24:32])
	reader.footer.TxnIDsCount = binary.LittleEndian.Uint64(footerBytes[32:40])
	reader.footer.Magic = binary.LittleEndian.Uint64(footerBytes[40:48])

	// Validate footer magic
	if reader.footer.Magic != MagicBytes {
		file.Close()
		return nil, fmt.Errorf("invalid file format: footer magic mismatch")
	}

	// Load the index for fast access
	if err := reader.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	// Read schema
	if err := reader.readSchema(); err != nil {
		file.Close()
		return nil, err
	}

	// Load committed transaction IDs if available
	if reader.footer.TxnIDsCount > 0 && reader.footer.TxnIDsOffset > 0 {
		if err := reader.loadCommittedTransactions(); err != nil {
			// Just log the error but continue - this is not critical for functioning
			fmt.Printf("Warning: Failed to load committed transactions: %v\n", err)
		}
	}

	return reader, nil
}

// Close closes the reader
func (r *DiskReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// loadIndex loads the index from the file
func (r *DiskReader) loadIndex() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Calculate the number of index entries
	numEntries := r.footer.IndexSize / IndexEntrySize

	// Allocate buffer for the index
	indexBytes := make([]byte, r.footer.IndexSize)

	// Read the index
	if _, err := r.file.ReadAt(indexBytes, int64(r.footer.IndexOffset)); err != nil {
		return err
	}

	// Create optimized B-Tree index
	r.index = btree.NewInt64BTree[int64]()

	// Parse index entries and prepare for batch insertion
	keys := make([]int64, numEntries)
	values := make([]int64, numEntries)

	for i := uint64(0); i < numEntries; i++ {
		offset := i * IndexEntrySize
		keys[i] = int64(binary.LittleEndian.Uint64(indexBytes[offset : offset+8]))
		values[i] = int64(binary.LittleEndian.Uint64(indexBytes[offset+8 : offset+16]))
	}

	// Use optimized batch insertion
	r.index.BatchInsert(keys, values)

	return nil
}

// loadCommittedTransactions loads the committed transaction IDs from the file
func (r *DiskReader) loadCommittedTransactions() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Calculate the number of transactions and total bytes
	numTxns := r.footer.TxnIDsCount
	txnIDsBytes := make([]byte, numTxns*16) // Each entry is 16 bytes (TxnID + Timestamp)

	// Read transaction IDs
	if _, err := r.file.ReadAt(txnIDsBytes, int64(r.footer.TxnIDsOffset)); err != nil {
		return fmt.Errorf("failed to read transaction IDs: %w", err)
	}

	// Process each transaction ID and its timestamp
	for i := uint64(0); i < numTxns; i++ {
		offset := i * 16
		txnID := int64(binary.LittleEndian.Uint64(txnIDsBytes[offset : offset+8]))
		timestamp := int64(binary.LittleEndian.Uint64(txnIDsBytes[offset+8 : offset+16]))

		r.committedTxnIDs[txnID] = timestamp
	}

	return nil
}

// readSchema reads the schema from the file
func (r *DiskReader) readSchema() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip header
	offset := int64(FileHeaderSize)

	// Read schema length
	lenBytes := make([]byte, 4)
	if _, err := r.file.ReadAt(lenBytes, offset); err != nil {
		return err
	}
	schemaLen := binary.LittleEndian.Uint32(lenBytes)
	offset += 4

	// Read schema data
	schemaBytes := make([]byte, schemaLen)
	if _, err := r.file.ReadAt(schemaBytes, offset); err != nil {
		return err
	}

	// Deserialize schema
	schema, err := deserializeSchema(schemaBytes)
	if err != nil {
		return err
	}

	r.schema = schema
	return nil
}

// GetRow retrieves a row version by rowID
func (r *DiskReader) GetRow(rowID int64) (RowVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Using B-Tree for faster lookup
	offset, found := r.index.Search(rowID)

	if !found {
		return RowVersion{}, false
	}

	// Read length prefix using pre-allocated buffer
	if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
		return RowVersion{}, false
	}
	rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

	// Read row data
	rowBytes := make([]byte, rowLen)
	if _, err := r.file.ReadAt(rowBytes, offset+4); err != nil {
		return RowVersion{}, false
	}

	// Deserialize row version
	version, err := deserializeRowVersion(rowBytes)
	if err != nil {
		return RowVersion{}, false
	}

	return version, true
}

// GetRowBatch retrieves multiple row versions by rowIDs
func (r *DiskReader) GetRowBatch(rowIDs []int64) map[int64]RowVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(rowIDs) == 0 {
		return nil
	}

	result := make(map[int64]RowVersion, len(rowIDs))

	for _, rowID := range rowIDs {
		// Using B-Tree for faster lookup
		offset, found := r.index.Search(rowID)
		if !found {
			continue
		}

		// Read length prefix
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			continue
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Read row data
		rowBytes := make([]byte, rowLen)
		if _, err := r.file.ReadAt(rowBytes, offset+4); err != nil {
			continue
		}

		// Deserialize row version
		version, err := deserializeRowVersion(rowBytes)
		if err != nil {
			continue
		}

		result[rowID] = version
	}

	return result
}

// GetAllRows retrieves all rows in the file
// This is used for full table scans from disk
func (r *DiskReader) GetAllRows() map[int64]RowVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Estimate initial capacity based on row count
	result := make(map[int64]RowVersion, r.footer.RowCount)

	// Iterate through all entries in the index
	r.index.ForEach(func(rowID int64, offset int64) bool {
		// Read length prefix using pre-allocated buffer
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			return true // Continue on error
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Read row data
		rowBytes := make([]byte, rowLen)
		if _, err := r.file.ReadAt(rowBytes, offset+4); err != nil {
			return true // Continue on error
		}

		// Deserialize row version
		version, err := deserializeRowVersion(rowBytes)
		if err != nil {
			return true // Continue on error
		}

		// Add to result
		result[rowID] = version
		return true
	})

	return result
}

// GetSchema returns the schema
func (r *DiskReader) GetSchema() *storage.Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schema
}

// GetCommittedTransactions returns the map of committed transaction IDs and their timestamps
func (r *DiskReader) GetCommittedTransactions() map[int64]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a copy to avoid concurrent map access issues
	result := make(map[int64]int64, len(r.committedTxnIDs))
	for txnID, timestamp := range r.committedTxnIDs {
		result[txnID] = timestamp
	}

	return result
}

// Helper functions for serialization

// serializeSchema serializes a schema to binary format
func serializeSchema(schema *storage.Schema) ([]byte, error) {
	writer := binser.NewWriter()
	defer writer.Release()

	// Write table name
	writer.WriteString(schema.TableName)

	// Write number of columns
	writer.WriteUint16(uint16(len(schema.Columns)))

	// Write each column
	for _, col := range schema.Columns {
		writer.WriteString(col.Name)
		writer.WriteUint8(uint8(col.Type))
		writer.WriteBool(col.Nullable)
		writer.WriteBool(col.PrimaryKey)
	}

	// Write timestamps
	writer.WriteInt64(schema.CreatedAt.UnixNano())
	writer.WriteInt64(schema.UpdatedAt.UnixNano())

	return writer.Bytes(), nil
}

// deserializeSchema deserializes a schema from binary data
func deserializeSchema(data []byte) (*storage.Schema, error) {
	reader := binser.NewReader(data)

	schema := &storage.Schema{}

	// Read table name
	tableName, err := reader.ReadString()
	if err != nil {
		return nil, err
	}
	schema.TableName = tableName

	// Read column count
	colCount, err := reader.ReadUint16()
	if err != nil {
		return nil, err
	}

	// Read columns
	schema.Columns = make([]storage.SchemaColumn, colCount)
	for i := 0; i < int(colCount); i++ {
		// Read column name
		colName, err := reader.ReadString()
		if err != nil {
			return nil, err
		}

		// Read column type
		colType, err := reader.ReadUint8()
		if err != nil {
			return nil, err
		}

		// Read nullable flag
		nullable, err := reader.ReadBool()
		if err != nil {
			return nil, err
		}

		// Read primary key flag
		primaryKey, err := reader.ReadBool()
		if err != nil {
			return nil, err
		}

		schema.Columns[i] = storage.SchemaColumn{
			ID:         i,
			Name:       colName,
			Type:       storage.DataType(colType),
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		}
	}

	// Read timestamps
	createdAt, err := reader.ReadInt64()
	if err != nil {
		return nil, err
	}
	schema.CreatedAt = time.Unix(0, createdAt)

	updatedAt, err := reader.ReadInt64()
	if err != nil {
		return nil, err
	}
	schema.UpdatedAt = time.Unix(0, updatedAt)

	return schema, nil
}

// serializeRow serializes a row to binary format
func serializeRow(row storage.Row) ([]byte, error) {
	buf := make([]byte, 0, 256) // Initial buffer size

	// Add column count
	buf = append(buf, byte(len(row)>>8), byte(len(row)))

	// Add each column
	for _, col := range row {
		if col == nil || col.IsNull() {
			// Write NULL marker
			buf = append(buf, binser.TypeNull)
			continue
		}

		// Write the column type
		buf = append(buf, binser.TypeUint8)
		buf = append(buf, byte(col.Type()))

		// Write value based on type
		switch col.Type() {
		case storage.TypeInteger:
			v, _ := col.AsInt64()
			buf = append(buf, binser.TypeInt64)
			// Allocate space for the int64 value
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buf[len(buf)-8:], uint64(v))

		case storage.TypeFloat:
			v, _ := col.AsFloat64()
			buf = append(buf, binser.TypeFloat64)
			// Allocate space for the float64 value
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buf[len(buf)-8:], math.Float64bits(v))

		case storage.TypeString:
			v, _ := col.AsString()
			buf = append(buf, binser.TypeString)
			// Allocate space for string length (uint32)
			buf = append(buf, 0, 0, 0, 0)
			// Write the length
			binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(v)))
			// Append the string data
			buf = append(buf, v...)

		case storage.TypeBoolean:
			v, _ := col.AsBoolean()
			buf = append(buf, binser.TypeBool)
			if v {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}

		case storage.TypeTimestamp:
			v, _ := col.AsTimestamp()
			buf = append(buf, binser.TypeTime)
			// Allocate space for the time value (int64 nanoseconds)
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buf[len(buf)-8:], uint64(v.UnixNano()))

		case storage.TypeDate:
			v, _ := col.AsDate()
			buf = append(buf, binser.TypeTime)
			// Allocate space for the date value (int64 nanoseconds)
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buf[len(buf)-8:], uint64(v.UnixNano()))

		case storage.TypeTime:
			v, _ := col.AsTime()
			buf = append(buf, binser.TypeTime)
			// Allocate space for the time value (int64 nanoseconds)
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buf[len(buf)-8:], uint64(v.UnixNano()))

		case storage.TypeJSON:
			v, _ := col.AsJSON()
			buf = append(buf, binser.TypeString)
			// Allocate space for JSON string length (uint32)
			buf = append(buf, 0, 0, 0, 0)
			// Write the length
			binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(v)))
			// Append the JSON string data
			buf = append(buf, v...)

		default:
			return nil, fmt.Errorf("unsupported type: %v", col.Type())
		}
	}

	return buf, nil
}

// serializeRowVersion serializes a row version to binary format
// This reuses the same approach as in the snapshotter for consistency
func serializeRowVersion(version RowVersion) ([]byte, error) {
	writer := binser.NewWriter()
	defer writer.Release()

	// Write basic fields
	writer.WriteInt64(version.RowID)
	writer.WriteInt64(version.TxnID)
	writer.WriteInt64(version.CreateTime)
	writer.WriteBool(version.IsDeleted)

	// Write row data if not deleted
	if !version.IsDeleted && version.Data != nil {
		rowData, err := serializeRow(version.Data)
		if err != nil {
			return nil, err
		}
		writer.WriteBytes(rowData)
	} else {
		// Empty data
		writer.WriteBytes(nil)
	}

	return writer.Bytes(), nil
}

func deserializeRow(data []byte) (storage.Row, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid row data: too short")
	}

	// Read column count
	colCount := int(uint16(data[0])<<8 | uint16(data[1]))
	pos := 2

	// Create row with pre-allocated capacity
	row := make(storage.Row, colCount)

	// Read each column
	for i := 0; i < colCount; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of data at column %d", i)
		}

		// Read type marker
		marker := data[pos]
		pos++

		if marker == binser.TypeNull {
			// NULL value
			row[i] = storage.StaticNullUnknown
			continue
		}

		if marker == binser.TypeUint8 {
			// Our custom format - read column type
			if pos >= len(data) {
				return nil, fmt.Errorf("unexpected end of data while reading column type for column %d", i)
			}
			colType := storage.DataType(data[pos])
			pos++

			// Bounds checking helper function
			ensureSpace := func(needed int, description string) error {
				if pos+needed > len(data) {
					return fmt.Errorf("unexpected end of data while reading %s for column %d", description, i)
				}
				return nil
			}

			// Read value based on type
			switch colType {
			case storage.TypeInteger:
				if err := ensureSpace(9, "integer"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeInt64 {
					return nil, fmt.Errorf("invalid integer format for column %d", i)
				}
				pos++
				val := int64(binary.LittleEndian.Uint64(data[pos:]))
				row[i] = storage.NewIntegerValue(val)
				pos += 8

			case storage.TypeFloat:
				if err := ensureSpace(9, "float"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeFloat64 {
					return nil, fmt.Errorf("invalid float format for column %d", i)
				}
				pos++
				bits := binary.LittleEndian.Uint64(data[pos:])
				val := math.Float64frombits(bits)
				row[i] = storage.NewFloatValue(val)
				pos += 8

			case storage.TypeString:
				if err := ensureSpace(1, "string marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeString {
					return nil, fmt.Errorf("invalid string format for column %d", i)
				}
				pos++
				if err := ensureSpace(4, "string length"); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if err := ensureSpace(int(length), "string data"); err != nil {
					return nil, err
				}
				val := string(data[pos : pos+int(length)])
				row[i] = storage.NewStringValue(val)
				pos += int(length)

			case storage.TypeBoolean:
				if err := ensureSpace(1, "boolean marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeBool {
					return nil, fmt.Errorf("invalid boolean format for column %d", i)
				}
				pos++
				if err := ensureSpace(1, "boolean value"); err != nil {
					return nil, err
				}
				val := data[pos] != 0
				row[i] = storage.NewBooleanValue(val)
				pos++

			case storage.TypeTimestamp, storage.TypeDate, storage.TypeTime:
				if err := ensureSpace(1, "time marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeTime {
					return nil, fmt.Errorf("invalid time format for column %d", i)
				}
				pos++
				if err := ensureSpace(8, "time value"); err != nil {
					return nil, err
				}
				nanos := int64(binary.LittleEndian.Uint64(data[pos:]))
				val := time.Unix(0, nanos)
				pos += 8

				// Create appropriate time-based value
				switch colType {
				case storage.TypeTimestamp:
					row[i] = storage.NewTimestampValue(val)
				case storage.TypeDate:
					row[i] = storage.NewDateValue(val)
				case storage.TypeTime:
					row[i] = storage.NewTimeValue(val)
				}

			case storage.TypeJSON:
				if err := ensureSpace(1, "JSON marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeString {
					return nil, fmt.Errorf("invalid JSON format for column %d", i)
				}
				pos++
				if err := ensureSpace(4, "JSON length"); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if err := ensureSpace(int(length), "JSON data"); err != nil {
					return nil, err
				}
				val := string(data[pos : pos+int(length)])
				row[i] = storage.NewJSONValue(val)
				pos += int(length)

			default:
				return nil, fmt.Errorf("unsupported column type: %d for column %d", colType, i)
			}
		} else {
			// Legacy format - return a clear error
			return nil, fmt.Errorf("legacy format not supported in deserializeRow for column %d", i)
		}
	}

	return row, nil
}

// deserializeRowVersion deserializes a row version from binary data
// This reuses the same approach as in the snapshotter for consistency
func deserializeRowVersion(data []byte) (RowVersion, error) {
	reader := binser.NewReader(data)

	var version RowVersion

	// Read basic fields
	rowID, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.RowID = rowID

	txnID, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.TxnID = txnID

	createTime, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.CreateTime = createTime

	isDeleted, err := reader.ReadBool()
	if err != nil {
		return version, err
	}
	version.IsDeleted = isDeleted

	// Read row data if not deleted
	rowData, err := reader.ReadBytes()
	if err != nil {
		return version, err
	}

	if !isDeleted && len(rowData) > 0 {
		version.Data, err = deserializeRow(rowData)
		if err != nil {
			return version, err
		}
	}

	return version, nil
}

// schemaHash computes a hash of the schema for validation
func schemaHash(schema interface{}) uint64 {
	var schemaToHash storage.Schema

	// Handle pointer or value
	switch s := schema.(type) {
	case storage.Schema:
		schemaToHash = s
	case *storage.Schema:
		if s != nil {
			schemaToHash = *s
		} else {
			return 0 // Nil schema
		}
	default:
		return 0 // Unknown type
	}

	var hash uint64 = 14695981039346656037 // FNV offset basis

	// Hash table name
	for _, c := range schemaToHash.TableName {
		hash ^= uint64(c)
		hash *= 1099511628211 // FNV prime
	}

	// Hash columns
	for _, col := range schemaToHash.Columns {
		// Hash column name
		for _, c := range col.Name {
			hash ^= uint64(c)
			hash *= 1099511628211
		}

		// Hash column type
		hash ^= uint64(col.Type)
		hash *= 1099511628211

		// Hash nullable flag
		if col.Nullable {
			hash ^= 1
		}
		hash *= 1099511628211

		// Hash primary key flag
		if col.PrimaryKey {
			hash ^= 1
		}
		hash *= 1099511628211
	}

	return hash
}
