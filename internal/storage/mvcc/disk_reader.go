package mvcc

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/stoolap/stoolap/internal/btree"
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

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

	// Loaded rowids tracking
	LoadedRowIDs *fastmap.FastInt64Map[struct{}] // Maps loaded rowIDs

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
		file:         file,
		filePath:     filePath,
		fileSize:     stat.Size(),
		lenBuffer:    make([]byte, 4),                      // Pre-allocate buffer for length prefix
		LoadedRowIDs: fastmap.NewFastInt64Map[struct{}](8), // Initialize loaded rowids map
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

	if _, ok := r.LoadedRowIDs.Get(rowID); ok {
		return RowVersion{}, false // Already loaded
	}

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

	// Mark this rowID as loaded
	r.LoadedRowIDs.Set(rowID, struct{}{})

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
		if _, ok := r.LoadedRowIDs.Get(rowID); ok {
			continue // Already loaded
		}

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

		// Mark this rowID as loaded
		r.LoadedRowIDs.Set(rowID, struct{}{})

		result[rowID] = version
	}

	return result
}

// ForEach iterates through all rows in the file and calls the provided function for each row
// This is more memory-efficient than GetAllRows as it doesn't create a map of all rows
func (r *DiskReader) ForEach(callback func(rowID int64, version RowVersion) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if callback == nil || r.index == nil {
		return
	}

	// Track process counts for debugging/monitoring
	processed := 0
	errors := 0

	// Iterate through all entries in the index
	r.index.ForEach(func(rowID int64, offset int64) bool {
		// Check if we've already loaded this rowID
		_, alreadyLoaded := r.LoadedRowIDs.Get(rowID)
		if alreadyLoaded {
			return true // Skip rows we've already loaded into memory
		}

		// Read length prefix using pre-allocated buffer
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			errors++
			return true // Continue on error
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Read row data
		rowBytes := make([]byte, rowLen)
		if _, err := r.file.ReadAt(rowBytes, offset+4); err != nil {
			errors++
			return true // Continue on error
		}

		// Deserialize row version
		version, err := deserializeRowVersion(rowBytes)
		if err != nil {
			errors++
			return true // Continue on error
		}

		processed++

		// Mark this rowID as processed
		r.LoadedRowIDs.Set(rowID, struct{}{})

		// Call the callback with the rowID and version
		return callback(rowID, version)
	})

	// Optional debug info if needed
	if errors > 0 {
		fmt.Printf("Error: DiskReader processed %d rows, encountered %d errors\n",
			processed, errors)
	}
}

// GetAllRows retrieves all rows in the file
// This is used for full table scans from disk
func (r *DiskReader) GetAllRows() map[int64]RowVersion {
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
