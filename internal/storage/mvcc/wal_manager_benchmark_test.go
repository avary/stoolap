package mvcc

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// Test data for benchmarks
var legacyWMdata []WALEntry

func init() {
	// Generate test data once
	legacyWMdata = generateLegacySimplifiedMockWALEntries(100, 1)
}

func generateLegacySimplifiedMockWALEntries(count int, dataSizeKB int) []WALEntry {
	entries := make([]WALEntry, count)
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	dataSizeBytes := dataSizeKB * 1024

	mockTableNames := []string{"table1", "table2", "table3"}
	allOperationTypes := []WALOperationType{
		WALInsert, WALUpdate, WALDelete, WALCommit, WALRollback,
		WALCreateTable, WALDropTable, WALAlterTable, WALCreateIndex, WALDropIndex,
	}

	for i := 0; i < count; i++ {
		opType := allOperationTypes[seededRand.Intn(len(allOperationTypes))]
		var entryData []byte = nil

		// Generate 1KB data for most operations
		if opType != WALCommit && opType != WALRollback {
			data := make([]byte, dataSizeBytes)
			for j := 0; j < dataSizeBytes; j++ {
				data[j] = byte(seededRand.Intn(256)) // Fill with random bytes
			}
			entryData = data
		}

		entries[i] = WALEntry{
			LSN:       uint64(i + 1),
			TxnID:     int64(i/10 + 1), // Simple TxnID logic
			TableName: mockTableNames[seededRand.Intn(len(mockTableNames))],
			RowID:     seededRand.Int63n(1000000) + 1,
			Operation: opType,
			Data:      entryData,
			Timestamp: time.Now().UnixNano() - int64(seededRand.Intn(100000000)), // Slightly varied timestamps
		}
	}
	return entries
}

func BenchmarkWALMarshalFilePerf(b *testing.B) {
	tmpDir := os.TempDir() + "/" + fmt.Sprintf("%v", rand.Int63())
	wm, err := NewWALManager(tmpDir, SyncNone, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		wm.Close()
		os.RemoveAll(tmpDir)
	}()

	const workers = 5
	chunk := len(legacyWMdata) / workers

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(workers)

		for w := 0; w < workers; w++ {
			start := w * chunk
			end := start + chunk
			if w == workers-1 { // last worker takes the remainder
				end = len(legacyWMdata)
			}

			go func(s, e int) {
				defer wg.Done()
				for _, entry := range legacyWMdata[s:e] {
					if _, err := wm.AppendEntry(entry); err != nil {
						b.Fatal(err)
					}
				}
			}(start, end)
		}
		wg.Wait()
	}
}

// Benchmark for serialization only
func BenchmarkWALSerializeEntry(b *testing.B) {
	wm := &WALManager{}
	entry := WALEntry{
		LSN:       12345,
		TxnID:     67890,
		TableName: "test_table_with_longer_name",
		RowID:     111213,
		Operation: WALInsert,
		Data:      make([]byte, 1024), // 1KB of data
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wm.serializeEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark for batch operations
func BenchmarkWALBatchCommit(b *testing.B) {
	tmpDir := os.TempDir() + "/" + fmt.Sprintf("%v", rand.Int63())
	wm, err := NewWALManager(tmpDir, SyncNone, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		wm.Close()
		os.RemoveAll(tmpDir)
	}()

	// Create batches
	batchSize := 100
	entries := generateLegacySimplifiedMockWALEntries(batchSize, 1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := wm.BatchCommit(entries)
		if err != nil {
			b.Fatal(err)
		}
	}
}
