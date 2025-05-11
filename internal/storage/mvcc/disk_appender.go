package mvcc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/stoolap/stoolap/internal/storage"
)

// DiskAppender handles writing row versions to disk
type DiskAppender struct {
	file            *os.File
	writer          *bufio.Writer
	indexBuffer     *bytes.Buffer
	dataOffset      uint64
	rowCount        uint64
	rowIDIndex      map[int64]uint64 // Maps rowIDs to offsets
	committedTxnIDs map[int64]int64  // Maps committed TxnIDs to timestamps

	fails bool // Indicates if the appender has failed
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

		os.Remove(filePath) // Clean up on error
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

// Fail marks the appender as failed, preventing further writes
func (a *DiskAppender) Fail() {
	a.fails = true
}

// Close closes the appender and its file
func (a *DiskAppender) Close() error {
	if a.file != nil {
		if a.writer != nil {
			a.writer.Flush()
		}

		fileName := a.file.Name() // Store the filename before closing
		closeErr := a.file.Close()

		if a.fails {
			removeErr := os.Remove(fileName)
			if closeErr != nil {
				return fmt.Errorf("failed to close file: %w (and tried to remove it)", closeErr)
			}
			return removeErr
		}

		return closeErr
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
