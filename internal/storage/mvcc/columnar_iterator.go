package mvcc

import (
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// ColumnarIndexIterator provides direct and efficient iteration over columnar index matches
type ColumnarIndexIterator struct {
	versionStore  *VersionStore
	txnID         int64
	schema        storage.Schema
	columnIndices []int

	// For direct sorted iteration
	rowIDs    []int64
	idIndex   int
	batchSize int

	// For current row
	currentRow   storage.Row
	projectedRow storage.Row

	// For pre-fetching
	prefetchRowIDs []int64
	prefetchIndex  int
	prefetchMap    *fastmap.Int64Map[storage.Row]
}

// NewColumnarIndexIterator creates an efficient iterator over the matched row IDs
func NewColumnarIndexIterator(
	versionStore *VersionStore,
	rowIDs []int64,
	txnID int64,
	schema storage.Schema,
	columnIndices []int) storage.Scanner {

	SIMDSortInt64s(rowIDs)

	return &ColumnarIndexIterator{
		versionStore:  versionStore,
		txnID:         txnID,
		schema:        schema,
		columnIndices: columnIndices,
		rowIDs:        rowIDs,
		idIndex:       -1,
		batchSize:     100, // Tune based on testing
		prefetchIndex: 0,
		projectedRow:  make(storage.Row, len(columnIndices)),
		prefetchMap:   GetRowMap(),
	}
}

// Next advances to the next matching row
func (it *ColumnarIndexIterator) Next() bool {
	// Check if we need to prefetch the next batch
	if it.prefetchIndex >= len(it.prefetchRowIDs) {
		// Calculate next batch to prefetch
		remainingRows := len(it.rowIDs) - it.idIndex - 1
		if remainingRows <= 0 {
			return false // No more rows
		}

		// Determine batch size (don't exceed remaining rows)
		batchSize := it.batchSize
		if remainingRows < batchSize {
			batchSize = remainingRows
		}

		// Clear previous batch data
		it.prefetchMap.Clear()

		// Prefetch batch of IDs
		endIdx := it.idIndex + 1 + batchSize
		if endIdx > len(it.rowIDs) {
			endIdx = len(it.rowIDs)
		}

		it.prefetchRowIDs = it.rowIDs[it.idIndex+1 : endIdx]
		it.prefetchIndex = 0

		// Batch fetch visible versions
		versions := it.versionStore.GetVisibleVersionsByIDs(it.prefetchRowIDs, it.txnID)

		// Extract non-deleted rows
		versions.ForEach(func(rowID int64, version *RowVersion) bool {
			if !version.IsDeleted {
				it.prefetchMap.Put(rowID, version.Data)
			}

			return true
		})

		// IMPORTANT: Free the versions map to avoid memory leak
		ReturnVisibleVersionMap(versions)

		// If nothing found, we're done
		if it.prefetchMap.Len() == 0 {
			return false
		}
	}

	// Advance until we find a visible row
	for it.prefetchIndex < len(it.prefetchRowIDs) {
		rowID := it.prefetchRowIDs[it.prefetchIndex]
		it.prefetchIndex++
		it.idIndex++

		// Check if this row is in our prefetch map
		if row, exists := it.prefetchMap.Get(rowID); exists {
			it.currentRow = row
			return true
		}
	}

	// Try next batch
	return it.Next()
}

// Row returns the current row
func (it *ColumnarIndexIterator) Row() storage.Row {
	if len(it.columnIndices) == 0 {
		return it.currentRow
	}

	// Project columns
	for i, colIdx := range it.columnIndices {
		if colIdx < len(it.currentRow) {
			it.projectedRow[i] = it.currentRow[colIdx]
		}
	}
	return it.projectedRow
}

// Err returns any error
func (it *ColumnarIndexIterator) Err() error {
	return nil
}

// Close releases resources
func (it *ColumnarIndexIterator) Close() error {
	clear(it.projectedRow)
	it.projectedRow = it.projectedRow[:0]

	it.rowIDs = nil
	it.prefetchRowIDs = nil
	it.currentRow = nil

	it.prefetchMap.Clear()
	PutRowMap(it.prefetchMap)

	return nil
}

// Helper for direct access to row IDs from columnar index
func GetRowIDsFromColumnarIndex(expr storage.Expression, index storage.Index) []int64 {
	// Extract the column name from the index
	var indexColumnName string
	if bi, ok := index.(*ColumnarIndex); ok {
		indexColumnName = bi.columnName
	}

	// If the expression is a SimpleExpression, check if it matches the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok && indexColumnName != "" {
		// If the expression is for this specific column, extract only that condition
		if simpleExpr.Column == indexColumnName {
			// Use type assertion to handle different index implementations
			if concreteIndex, ok := index.(*ColumnarIndex); ok {
				return getRowIDsFromColumnarIndex(simpleExpr, concreteIndex)
			} else {
				// For any other index that implements FilteredRowIDs interface
				if filteredIndex, ok := index.(interface {
					GetFilteredRowIDs(expr storage.Expression) []int64
				}); ok {
					return filteredIndex.GetFilteredRowIDs(simpleExpr)
				}
			}
		}
	}

	// For AND expressions, extract the condition for this specific column if possible
	if andExpr, ok := expr.(*expression.AndExpression); ok && indexColumnName != "" {
		for _, subExpr := range andExpr.Expressions {
			if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
				if simpleExpr.Column == indexColumnName {
					// Use type assertion to handle different index implementations
					if concreteIndex, ok := index.(*ColumnarIndex); ok {
						return getRowIDsFromColumnarIndex(simpleExpr, concreteIndex)
					} else {
						// For any other index that implements FilteredRowIDs interface
						if filteredIndex, ok := index.(interface {
							GetFilteredRowIDs(expr storage.Expression) []int64
						}); ok {
							return filteredIndex.GetFilteredRowIDs(simpleExpr)
						}
					}
				}
			}
		}
	}

	// Fall back to the default behavior for more complex expressions
	if concreteIndex, ok := index.(*ColumnarIndex); ok {
		return getRowIDsFromColumnarIndex(expr, concreteIndex)
	} else {
		// For any other index that implements FilteredRowIDs interface
		if filteredIndex, ok := index.(interface {
			GetFilteredRowIDs(expr storage.Expression) []int64
		}); ok {
			return filteredIndex.GetFilteredRowIDs(expr)
		}
		return nil
	}
}

// Helper for columnar index implementation
func getRowIDsFromColumnarIndex(expr storage.Expression, index *ColumnarIndex) []int64 {
	// Ultra-fast path for direct equality comparison on the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok &&
		simpleExpr.Column == index.columnName && simpleExpr.Operator == storage.EQ {

		exprValue := storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
		defer storage.PutPooledColumnValue(exprValue)
		// Fast equality match using our B-tree implementation
		return index.GetRowIDsEqual([]storage.ColumnValue{exprValue})
	}

	// For simple expressions on the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok &&
		simpleExpr.Column == index.columnName {

		switch simpleExpr.Operator {
		case storage.GT, storage.GTE, storage.LT, storage.LTE:
			// Range query
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool

			if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
				minValue = storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = simpleExpr.Operator == storage.GTE
			} else {
				maxValue = storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = simpleExpr.Operator == storage.LTE
			}

			// Use the index's range function
			return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

		case storage.ISNULL, storage.ISNOTNULL:
			// Use direct implementation in the B-tree index
			return index.GetFilteredRowIDs(simpleExpr)
		}
	}

	// Fast path for AND expressions on a single column
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		// Check if both expressions are on the same column as the index
		if ok1 && ok2 &&
			expr1.Column == index.columnName &&
			expr2.Column == index.columnName {

			// Look for range patterns: (col > X AND col < Y) or similar
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool
			var hasRange bool

			// Check if expr1 is a lower bound and expr2 is an upper bound
			if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
				(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr1.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr1.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr2.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr2.Operator == storage.LTE
				hasRange = true
			}

			// Check if expr2 is a lower bound and expr1 is an upper bound
			if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
				(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr2.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr2.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr1.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr1.Operator == storage.LTE
				hasRange = true
			}

			if hasRange {
				return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)
			}
		}
	}

	// Let the index handle the expression directly
	return index.GetFilteredRowIDs(expr)
}

// Helper to intersect sorted ID lists efficiently
func intersectSortedIDs(a []int64, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	// Sort if not already sorted
	if !isSorted(a) {
		SIMDSortInt64s(a)
	}

	if !isSorted(b) {
		SIMDSortInt64s(b)
	}

	// Use merge-like algorithm for linear time intersection
	result := make([]int64, 0, min(len(a), len(b)))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			// Equal values - add to result
			result = append(result, a[i])
			i++
			j++
		}
	}

	return result
}

// Helper to check if int64 slice is sorted
func isSorted(ids []int64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			return false
		}
	}
	return true
}
