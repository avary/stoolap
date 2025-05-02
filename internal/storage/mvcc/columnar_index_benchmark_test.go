package mvcc

import (
	"testing"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// createTestIndex creates a test columnar index with random data
func createTestIndex(size int, valuesPerRow int) *ColumnarIndex {
	index := NewColumnarIndex(
		"test_index",
		"test_table",
		"test_column",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add test data
	for rowID := int64(0); rowID < int64(size); rowID++ {
		// Add rowID % valuesPerRow as the value
		// This creates valuesPerRow different values distributed evenly
		value := storage.NewIntegerValue(rowID % int64(valuesPerRow))
		index.Add(value, rowID, 0)
	}

	return index
}

// createTestTimestampIndex creates a test columnar index with timestamp data
func createTestTimestampIndex(size int) *ColumnarIndex {
	index := NewColumnarIndex(
		"test_time_index",
		"test_table",
		"timestamp",
		0,
		storage.TIMESTAMP,
		nil,
		false,
	)

	// Add timestamp values with 1 day interval
	startDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := int64(0); i < int64(size); i++ {
		date := startDate.AddDate(0, 0, int(i))
		index.Add(storage.NewTimestampValue(date), i, 0)
	}

	return index
}

// BenchmarkColumnarIndexAllocFree compares original and allocation-free methods
func BenchmarkColumnarIndexAllocFree(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	valuesPerRow := 100 // Number of unique values

	for _, size := range sizes {
		b.Run("Size_"+string(rune(size)), func(b *testing.B) {
			index := createTestIndex(size, valuesPerRow)
			testValue := storage.NewIntegerValue(42) // Search for the value 42

			// Benchmark GetRowIDsEqual
			b.Run("GetRowIDsEqual", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetRowIDsEqual(testValue)
					// Consume the result to make sure work is done
					_ = len(rowIDs)
				}
			})

			// Use a range query to test range methods
			minValue := storage.NewIntegerValue(20)
			maxValue := storage.NewIntegerValue(60)

			// Benchmark GetRowIDsInRange
			b.Run("GetRowIDsInRange", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetRowIDsInRange(minValue, maxValue, true, true)
					// Consume the result to make sure work is done
					_ = len(rowIDs)
				}
			})

			// Benchmark operator methods
			b.Run("FindWithOperator", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					entries, _ := index.FindWithOperator(storage.GT, minValue)
					_ = len(entries)
				}
			})

			// Create a simple test expression for equality
			simpleExpr := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.EQ,
				Value:    int64(42),
			}

			// Create sample schema
			schema := storage.Schema{
				TableName: "test_table",
				Columns: []storage.SchemaColumn{
					{
						Name: "test_column",
						Type: storage.INTEGER,
					},
				},
			}

			// Create SchemaAwareExpression
			schemaExpr := expression.NewSchemaAwareExpression(simpleExpr, schema)
			schemaExpr.ColumnMap["test_column"] = 0 // Set column ID directly

			// Benchmark filtered methods
			b.Run("GetFilteredRowIDs", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(schemaExpr)
					_ = len(rowIDs)
				}
			})

			// Create complex expressions for AND/OR benchmarks
			simpleExpr1 := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.GT,
				Value:    int64(30),
			}

			simpleExpr2 := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.LT,
				Value:    int64(50),
			}

			// AND expression benchmark
			andExpr := expression.NewAndExpression(simpleExpr1, simpleExpr2)
			schemaAndExpr := expression.NewSchemaAwareExpression(andExpr, schema)
			schemaAndExpr.ColumnMap["test_column"] = 0 // Set column ID directly

			b.Run("GetFilteredRowIDs_And", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(schemaAndExpr)
					_ = len(rowIDs)
				}
			})

			// OR expression benchmark
			orExpr := expression.NewOrExpression(simpleExpr1, simpleExpr2)
			schemaOrExpr := expression.NewSchemaAwareExpression(orExpr, schema)
			schemaOrExpr.ColumnMap["test_column"] = 0 // Set column ID directly

			b.Run("GetFilteredRowIDs_Or", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(schemaOrExpr)
					_ = len(rowIDs)
				}
			})
		})
	}
}

// BenchmarkColumnarIndexTimestamp tests timestamp operations
func BenchmarkColumnarIndexTimestamp(b *testing.B) {
	index := createTestTimestampIndex(365 * 3) // ~3 years of daily data

	// Test timestamp equality
	b.Run("TimestampEquality", func(b *testing.B) {
		// Search for a specific date
		targetDate := time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.Find(storage.NewTimestampValue(targetDate))
			_ = len(entries)
		}
	})

	// Test timestamp range
	b.Run("TimestampRange", func(b *testing.B) {
		// Search for a 30-day range
		minDate := time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC)
		maxDate := time.Date(2021, 5, 31, 0, 0, 0, 0, time.UTC)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.FindRange(
				storage.NewTimestampValue(minDate),
				storage.NewTimestampValue(maxDate),
				true, true)
			_ = len(entries)
		}
	})

	// Test timestamp with operator
	b.Run("TimestampOperator", func(b *testing.B) {
		pivotDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.FindWithOperator(
				storage.GT, storage.NewTimestampValue(pivotDate))
			_ = len(entries)
		}
	})

	// Create schema for expression tests
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "timestamp", Type: storage.TIMESTAMP},
		},
	}

	// Test timestamp with GetFilteredRowIDs
	b.Run("TimestampFilteredRowIDs", func(b *testing.B) {
		targetDate := time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC)
		simpleExpr := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.EQ,
			Value:    targetDate,
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rowIDs := index.GetFilteredRowIDs(simpleExpr)
			_ = len(rowIDs)
		}
	})

	// Test timestamp with complex expressions
	b.Run("TimestampComplexFilter", func(b *testing.B) {
		// Range from 2021-01-01 to 2021-12-31
		startDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)

		expr1 := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.GTE,
			Value:    startDate,
		}
		expr2 := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.LTE,
			Value:    endDate,
		}

		andExpr := expression.NewAndExpression(expr1, expr2)
		schemaExpr := expression.NewSchemaAwareExpression(andExpr, schema)
		schemaExpr.ColumnMap["timestamp"] = 0 // Set column ID directly

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rowIDs := index.GetFilteredRowIDs(schemaExpr)
			_ = len(rowIDs)
		}
	})
}

// BenchmarkColumnarIndexSparseData tests performance with sparse data
func BenchmarkColumnarIndexSparseData(b *testing.B) {
	// Create a columnar index with very sparse data (many unique values)
	index := NewColumnarIndex(
		"sparse_index",
		"test_table",
		"sparse_column",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add sparse values - each row has a unique value
	for i := int64(0); i < 10000; i++ {
		index.Add(storage.NewIntegerValue(i), i, 0)
	}

	// Test with single lookups (worst case for bitmap operations)
	b.Run("SparseEquality", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			value := int64(i % 10000)
			entries, _ := index.Find(storage.NewIntegerValue(value))
			_ = len(entries)
		}
	})

	// Test with range queries (common case)
	b.Run("SparseRange", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			start := int64(i % 9000)
			end := start + 1000
			entries, _ := index.FindRange(
				storage.NewIntegerValue(start),
				storage.NewIntegerValue(end),
				true, true)
			_ = len(entries)
		}
	})

	// Test with GetFilteredRowIDs
	b.Run("SparseFilteredRowIDs", func(b *testing.B) {
		// Create schema
		schema := storage.Schema{
			TableName: "test_table",
			Columns: []storage.SchemaColumn{
				{Name: "sparse_column", Type: storage.INTEGER},
			},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			value := int64(i % 10000)
			expr := &expression.SimpleExpression{
				Column:   "sparse_column",
				Operator: storage.EQ,
				Value:    value,
			}
			schemaExpr := expression.NewSchemaAwareExpression(expr, schema)
			schemaExpr.ColumnMap["sparse_column"] = 0 // Set column ID directly

			rowIDs := index.GetFilteredRowIDs(schemaExpr)
			_ = len(rowIDs)
		}
	})
}
