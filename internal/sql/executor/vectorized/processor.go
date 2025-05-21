/* Copyright 2025 Stoolap Contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */

package vectorized

import (
	"math"
	"sync"
)

// Processor is the interface for all vector processing operations
type Processor interface {
	// Process takes a batch and returns a processed batch
	Process(batch *Batch) (*Batch, error)
}

// Initialize lookup tables for sin and cos functions
var sinLookupTable = initSinLookupTable()
var cosLookupTable = initCosLookupTable()

// initSinLookupTable initializes a lookup table with pre-computed sine values
func initSinLookupTable() [360]float64 {
	var table [360]float64
	for i := 0; i < 360; i++ {
		// Convert degrees to radians and compute sine
		table[i] = math.Sin(float64(i) * math.Pi / 180.0)
	}
	return table
}

// initCosLookupTable initializes a lookup table with pre-computed cosine values
func initCosLookupTable() [360]float64 {
	var table [360]float64
	for i := 0; i < 360; i++ {
		// Convert degrees to radians and compute cosine
		table[i] = math.Cos(float64(i) * math.Pi / 180.0)
	}
	return table
}

// VectorizedFilterProcessor implements filtering operations in a vectorized manner
type VectorizedFilterProcessor struct {
	// Predicate function that returns a bitmap of which rows pass the filter
	Predicate func(batch *Batch) ([]bool, error)
}

// Process applies the filter to the input batch
func (vfp *VectorizedFilterProcessor) Process(input *Batch) (*Batch, error) {
	// Apply the predicate to get the filtering bitmap
	selectionVector, err := vfp.Predicate(input)
	if err != nil {
		return nil, err
	}

	// Count how many rows pass the filter
	resultSize := 0
	for i := 0; i < input.Size; i++ {
		if selectionVector[i] {
			resultSize++
		}
	}

	// Create a new batch with only the filtered rows
	result := NewBatch(resultSize)

	// Copy all column schemas from input
	for _, colName := range input.ColumnNames {
		// Add columns of the appropriate type to the result batch
		if _, exists := input.IntColumns[colName]; exists {
			result.AddIntColumn(colName)
		} else if _, exists := input.FloatColumns[colName]; exists {
			result.AddFloatColumn(colName)
		} else if _, exists := input.StringColumns[colName]; exists {
			result.AddStringColumn(colName)
		} else if _, exists := input.BoolColumns[colName]; exists {
			result.AddBoolColumn(colName)
		} else if _, exists := input.TimeColumns[colName]; exists {
			result.AddTimeColumn(colName)
		}
	}

	// Resize result to hold filtered rows
	result.Size = resultSize

	// Copy data for rows that pass the filter
	resultIdx := 0
	for i := 0; i < input.Size; i++ {
		if selectionVector[i] {
			// Copy data for this row from input to result
			for _, colName := range input.ColumnNames {
				// Copy based on column type
				if _, exists := input.IntColumns[colName]; exists {
					result.IntColumns[colName][resultIdx] = input.IntColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = input.NullBitmaps[colName][i]
				} else if _, exists := input.FloatColumns[colName]; exists {
					result.FloatColumns[colName][resultIdx] = input.FloatColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = input.NullBitmaps[colName][i]
				} else if _, exists := input.StringColumns[colName]; exists {
					result.StringColumns[colName][resultIdx] = input.StringColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = input.NullBitmaps[colName][i]
				} else if _, exists := input.BoolColumns[colName]; exists {
					result.BoolColumns[colName][resultIdx] = input.BoolColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = input.NullBitmaps[colName][i]
				} else if _, exists := input.TimeColumns[colName]; exists {
					result.TimeColumns[colName][resultIdx] = input.TimeColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = input.NullBitmaps[colName][i]
				}
			}
			resultIdx++
		}
	}

	return result, nil
}

// SelectionVector is an efficient representation of selected rows
type SelectionVector []int

// selectionVectorPool is a pool of reusable selection vectors
var selectionVectorPool = sync.Pool{
	New: func() interface{} { return make(SelectionVector, 0, MaxBatchSize) },
}

// getSelectionVector gets a selection vector from the pool
func getSelectionVector(capacity int) SelectionVector {
	buf := selectionVectorPool.Get().(SelectionVector)
	if cap(buf) < capacity {
		buf = make(SelectionVector, 0, capacity)
	} else {
		buf = buf[:0]
	}
	return buf
}

// releaseSelectionVector returns a selection vector to the pool
func releaseSelectionVector(vec SelectionVector) {
	// Only return vectors with substantial capacity to the pool
	if cap(vec) >= MaxBatchSize/4 {
		selectionVectorPool.Put(vec[:0])
	}
}

// CompareConstantProcessor compares a column to a constant value
type CompareConstantProcessor struct {
	// The column to compare
	ColumnName string

	// The comparison operator (=, <>, >, >=, <, <=)
	Operator string

	// The constant value to compare against
	Value interface{}
}

// NewCompareConstantProcessor creates a new comparison processor
func NewCompareConstantProcessor(colName, op string, value interface{}) *CompareConstantProcessor {
	return &CompareConstantProcessor{
		ColumnName: colName,
		Operator:   op,
		Value:      value,
	}
}

// SIMDComparisonProcessor is a specialized processor for numeric comparisons
// that uses SIMD instructions for better performance
type SIMDComparisonProcessor struct {
	// The column to compare
	ColumnName string

	// The comparison operator (=, <>, >, >=, <, <=)
	Operator string

	// The constant value to compare against (must be float64)
	Value float64
}

// NewSIMDComparisonProcessor creates a new SIMD-optimized comparison processor for floating point operations
func NewSIMDComparisonProcessor(colName string, op string, value float64) *SIMDComparisonProcessor {
	return &SIMDComparisonProcessor{
		ColumnName: colName,
		Operator:   op,
		Value:      value,
	}
}

// getSelectionVector creates a selection vector using SIMD-optimized comparison
func (p *SIMDComparisonProcessor) getSelectionVector(batch *Batch) (SelectionVector, int) {
	selection := getSelectionVector(batch.Size)

	// Check both int and float columns
	if batch.HasIntColumn(p.ColumnName) {
		col := batch.IntColumns[p.ColumnName]
		nulls := batch.NullBitmaps[p.ColumnName]

		// Convert our floating point value to integer for comparison
		intValue := int64(p.Value)

		// Create a result buffer for the comparison
		resultBuffer := getBoolBuffer(batch.Size)
		defer nullBufferPool.Put(resultBuffer[:0]) // Return to pool when done

		// Apply the appropriate specialized SIMD comparison function for integers
		switch p.Operator {
		case "=", "==":
			CompareEQScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		case "!=", "<>":
			CompareNEScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		case ">":
			CompareGTScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		case ">=":
			CompareGEScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		case "<":
			CompareLTScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		case "<=":
			CompareLEScalarInt64SIMD(resultBuffer, col, intValue, batch.Size)
		default:
			// Fallback to simple loop for unsupported operators
			matchCount := 0
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] {
					var match bool
					switch p.Operator {
					case "=", "==":
						match = float64(col[i]) == p.Value
					case "!=", "<>":
						match = float64(col[i]) != p.Value
					case ">":
						match = float64(col[i]) > p.Value
					case ">=":
						match = float64(col[i]) >= p.Value
					case "<":
						match = float64(col[i]) < p.Value
					case "<=":
						match = float64(col[i]) <= p.Value
					}
					if match {
						selection = append(selection, i)
						matchCount++
					}
				}
			}
			return selection, matchCount
		}

		// Add matching row indices to selection vector, skipping NULLs
		matchCount := 0
		for i := 0; i < batch.Size; i++ {
			if !nulls[i] && resultBuffer[i] {
				selection = append(selection, i)
				matchCount++
			}
		}

		return selection, len(selection)
	}

	// Only process float columns with SIMD operations
	if !batch.HasFloatColumn(p.ColumnName) {
		return selection, 0
	}

	col := batch.FloatColumns[p.ColumnName]
	nulls := batch.NullBitmaps[p.ColumnName]

	// Create a result buffer for the comparison
	resultBuffer := getBoolBuffer(batch.Size)
	defer nullBufferPool.Put(resultBuffer[:0]) // Return to pool when done

	// Apply the appropriate SIMD comparison function based on operator
	// We use dedicated optimized functions for each comparison type
	switch p.Operator {
	case "=", "==":
		CompareEQScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	case "!=", "<>":
		CompareNEScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	case ">":
		CompareGTScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	case ">=":
		CompareGEScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	case "<":
		CompareLTScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	case "<=":
		CompareLEScalarFloat64SIMD(resultBuffer, col, p.Value, batch.Size)
	default:
		// Unsupported operator
		return selection, 0
	}

	// Add matching row indices to selection vector, skipping NULLs
	matchCount := 0
	for i := 0; i < batch.Size; i++ {
		if !nulls[i] && resultBuffer[i] {
			selection = append(selection, i)
			matchCount++
		}
	}

	return selection, len(selection)
}

// Process applies the comparison operation to the batch
func (p *SIMDComparisonProcessor) Process(batch *Batch) (*Batch, error) {
	// Get selection vector of matching rows
	selection, rowCount := p.getSelectionVector(batch)

	// Make sure to release the selection vector when we're done
	defer releaseSelectionVector(selection)

	// If no rows match, return an empty batch but preserve column names
	if rowCount == 0 {
		// Create empty result
		result := NewBatch(0)

		// Copy column names
		result.ColumnNames = make([]string, len(batch.ColumnNames))
		copy(result.ColumnNames, batch.ColumnNames)

		return result, nil
	}

	// Create result batch with the right size
	result := NewBatch(rowCount)

	// Copy column names first to ensure they're preserved
	result.ColumnNames = make([]string, len(batch.ColumnNames))
	copy(result.ColumnNames, batch.ColumnNames)

	// Add all columns
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			result.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			result.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			result.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			result.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			result.AddTimeColumn(colName)
		}
	}

	// Copy only the matching rows using the selection vector
	for outRow := 0; outRow < rowCount; outRow++ {
		srcRow := selection[outRow]

		// Copy all column values for this row
		for _, colName := range result.ColumnNames {
			if result.HasIntColumn(colName) {
				result.IntColumns[colName][outRow] = batch.IntColumns[colName][srcRow]
				result.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if result.HasFloatColumn(colName) {
				result.FloatColumns[colName][outRow] = batch.FloatColumns[colName][srcRow]
				result.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if result.HasStringColumn(colName) {
				result.StringColumns[colName][outRow] = batch.StringColumns[colName][srcRow]
				result.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if result.HasBoolColumn(colName) {
				result.BoolColumns[colName][outRow] = batch.BoolColumns[colName][srcRow]
				result.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if result.HasTimeColumn(colName) {
				result.TimeColumns[colName][outRow] = batch.TimeColumns[colName][srcRow]
				result.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			}
		}
	}

	return result, nil
}

// Process applies the comparison operation to the batch
func (p *CompareConstantProcessor) Process(batch *Batch) (*Batch, error) {
	// Special fastpath for comparing integer columns with simple conditions
	// This avoids creating a selection vector and new batch for common cases
	if batch.HasIntColumn(p.ColumnName) {
		if intVal, ok := p.Value.(int64); ok {
			return p.processIntComparisonFast(batch, intVal, p.Operator)
		} else if intVal, ok := p.Value.(int); ok {
			return p.processIntComparisonFast(batch, int64(intVal), p.Operator)
		}
	} else if batch.HasStringColumn(p.ColumnName) {
		if strVal, ok := p.Value.(string); ok && p.Operator == "=" {
			return p.processStringComparisonFast(batch, strVal)
		}
	}

	// Get a selection vector of which rows match
	selection, rowCount := p.getSelectionVector(batch)

	// Make sure to release the selection vector when we're done
	defer releaseSelectionVector(selection)

	// If no rows match, return an empty batch
	if rowCount == 0 {
		return NewBatch(0), nil
	}

	// Create output batch with only the selected rows
	output := NewBatch(rowCount)

	// Add all columns from the batch
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			output.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			output.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			output.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			output.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			output.AddTimeColumn(colName)
		}
	}

	// Copy only selected rows using the selection vector
	for outRow := 0; outRow < rowCount; outRow++ {
		srcRow := selection[outRow]

		// Copy values for this row
		for _, colName := range output.ColumnNames {
			if output.HasIntColumn(colName) {
				output.IntColumns[colName][outRow] = batch.IntColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasFloatColumn(colName) {
				output.FloatColumns[colName][outRow] = batch.FloatColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasStringColumn(colName) {
				output.StringColumns[colName][outRow] = batch.StringColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasBoolColumn(colName) {
				output.BoolColumns[colName][outRow] = batch.BoolColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasTimeColumn(colName) {
				output.TimeColumns[colName][outRow] = batch.TimeColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			}
		}
	}

	return output, nil
}

// processIntComparisonFast is a specialized fast path for integer comparisons
// This avoids the overhead of creating a selection vector for common cases
func (p *CompareConstantProcessor) processIntComparisonFast(batch *Batch, compareValue int64, op string) (*Batch, error) {
	col := batch.IntColumns[p.ColumnName]
	nulls := batch.NullBitmaps[p.ColumnName]

	// Count matching rows first to allocate exact size
	matchCount := 0
	for i := 0; i < batch.Size; i++ {
		if !nulls[i] {
			switch op {
			case ">":
				if col[i] > compareValue {
					matchCount++
				}
			case ">=":
				if col[i] >= compareValue {
					matchCount++
				}
			case "<":
				if col[i] < compareValue {
					matchCount++
				}
			case "<=":
				if col[i] <= compareValue {
					matchCount++
				}
			case "=", "==":
				if col[i] == compareValue {
					matchCount++
				}
			case "!=", "<>":
				if col[i] != compareValue {
					matchCount++
				}
			}
		}
	}

	// If no matches, return empty batch
	if matchCount == 0 {
		return NewBatch(0), nil
	}

	// Create result batch
	result := NewBatch(matchCount)

	// Copy column list - IMPORTANT to keep the same order of columns
	result.ColumnNames = make([]string, len(batch.ColumnNames))
	copy(result.ColumnNames, batch.ColumnNames)

	// Copy column structures - we copy all the column metadata, not just the data
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			result.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			result.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			result.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			result.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			result.AddTimeColumn(colName)
		}
	}

	// Copy matching rows
	resultIdx := 0
	for i := 0; i < batch.Size; i++ {
		// Check if this row matches
		isMatch := false
		if !nulls[i] {
			switch op {
			case ">":
				isMatch = col[i] > compareValue
			case ">=":
				isMatch = col[i] >= compareValue
			case "<":
				isMatch = col[i] < compareValue
			case "<=":
				isMatch = col[i] <= compareValue
			case "=", "==":
				isMatch = col[i] == compareValue
			case "!=", "<>":
				isMatch = col[i] != compareValue
			}
		}

		if isMatch {
			// Copy all column values for this row
			for _, colName := range result.ColumnNames {
				if result.HasIntColumn(colName) {
					result.IntColumns[colName][resultIdx] = batch.IntColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasFloatColumn(colName) {
					result.FloatColumns[colName][resultIdx] = batch.FloatColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasStringColumn(colName) {
					result.StringColumns[colName][resultIdx] = batch.StringColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasBoolColumn(colName) {
					result.BoolColumns[colName][resultIdx] = batch.BoolColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasTimeColumn(colName) {
					result.TimeColumns[colName][resultIdx] = batch.TimeColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				}
			}
			resultIdx++
		}
	}

	return result, nil
}

// processStringComparisonFast is a specialized fast path for string equality comparisons
func (p *CompareConstantProcessor) processStringComparisonFast(batch *Batch, compareValue string) (*Batch, error) {
	col := batch.StringColumns[p.ColumnName]
	nulls := batch.NullBitmaps[p.ColumnName]

	// Count matching rows first to allocate exact size
	matchCount := 0
	for i := 0; i < batch.Size; i++ {
		if !nulls[i] && col[i] == compareValue {
			matchCount++
		}
	}

	// If no matches, return empty batch
	if matchCount == 0 {
		return NewBatch(0), nil
	}

	// Create result batch
	result := NewBatch(matchCount)

	// Copy column list - IMPORTANT to keep the same order of columns
	result.ColumnNames = make([]string, len(batch.ColumnNames))
	copy(result.ColumnNames, batch.ColumnNames)

	// Copy column structures
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			result.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			result.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			result.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			result.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			result.AddTimeColumn(colName)
		}
	}

	// Copy matching rows
	resultIdx := 0
	for i := 0; i < batch.Size; i++ {
		// Check if this row matches (string equality)
		if !nulls[i] && col[i] == compareValue {
			// Copy all column values for this row
			for _, colName := range result.ColumnNames {
				if result.HasIntColumn(colName) {
					result.IntColumns[colName][resultIdx] = batch.IntColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasFloatColumn(colName) {
					result.FloatColumns[colName][resultIdx] = batch.FloatColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasStringColumn(colName) {
					result.StringColumns[colName][resultIdx] = batch.StringColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasBoolColumn(colName) {
					result.BoolColumns[colName][resultIdx] = batch.BoolColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				} else if result.HasTimeColumn(colName) {
					result.TimeColumns[colName][resultIdx] = batch.TimeColumns[colName][i]
					result.NullBitmaps[colName][resultIdx] = batch.NullBitmaps[colName][i]
				}
			}
			resultIdx++
		}
	}

	return result, nil
}

// getSelectionVector creates a selection vector of row indices that match the condition
func (p *CompareConstantProcessor) getSelectionVector(batch *Batch) (SelectionVector, int) {
	// Get a selection vector from the pool
	selection := getSelectionVector(batch.Size)

	// Apply the comparison based on the column type
	if batch.HasIntColumn(p.ColumnName) {
		// Integer column
		col := batch.IntColumns[p.ColumnName]
		nulls := batch.NullBitmaps[p.ColumnName]

		// Convert value to int64 if possible
		var val int64
		switch v := p.Value.(type) {
		case int64:
			val = v
		case int:
			val = int64(v)
		case float64:
			val = int64(v)
		default:
			// For non-numeric types, select no rows
			return selection, 0
		}

		// Apply the comparison based on operator
		matchCount := 0
		switch p.Operator {
		case "=", "==":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] == val {
					selection = append(selection, i)
					matchCount++
				}
			}
		case "!=", "<>":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] != val {
					selection = append(selection, i)
					matchCount++
				}
			}
		case ">":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] > val {
					selection = append(selection, i)
					matchCount++
				}
			}
		case ">=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] >= val {
					selection = append(selection, i)
					matchCount++
				}
			}
		case "<":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] < val {
					selection = append(selection, i)
					matchCount++
				}
			}
		case "<=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] <= val {
					selection = append(selection, i)
					matchCount++
				}
			}
		}
	} else if batch.HasFloatColumn(p.ColumnName) {
		// Float column
		col := batch.FloatColumns[p.ColumnName]
		nulls := batch.NullBitmaps[p.ColumnName]

		// Convert value to float64 if possible
		var val float64
		switch v := p.Value.(type) {
		case float64:
			val = v
		case int64:
			val = float64(v)
		case int:
			val = float64(v)
		default:
			// For non-numeric types, select no rows
			return selection, 0
		}

		// Apply the comparison based on operator
		switch p.Operator {
		case "=", "==":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] == val {
					selection = append(selection, i)
				}
			}
		case "!=", "<>":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] != val {
					selection = append(selection, i)
				}
			}
		case ">":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] > val {
					selection = append(selection, i)
				}
			}
		case ">=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] >= val {
					selection = append(selection, i)
				}
			}
		case "<":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] < val {
					selection = append(selection, i)
				}
			}
		case "<=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] <= val {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasStringColumn(p.ColumnName) {
		// String column
		col := batch.StringColumns[p.ColumnName]
		nulls := batch.NullBitmaps[p.ColumnName]

		// Convert value to string if possible
		var val string
		switch v := p.Value.(type) {
		case string:
			val = v
		default:
			// For non-string types, select no rows
			return selection, 0
		}

		// Apply the comparison based on operator
		switch p.Operator {
		case "=", "==":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] == val {
					selection = append(selection, i)
				}
			}
		case "!=", "<>":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] != val {
					selection = append(selection, i)
				}
			}
		case ">":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] > val {
					selection = append(selection, i)
				}
			}
		case ">=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] >= val {
					selection = append(selection, i)
				}
			}
		case "<":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] < val {
					selection = append(selection, i)
				}
			}
		case "<=":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] <= val {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasBoolColumn(p.ColumnName) {
		// Boolean column
		col := batch.BoolColumns[p.ColumnName]
		nulls := batch.NullBitmaps[p.ColumnName]

		// Convert value to bool if possible
		var val bool
		switch v := p.Value.(type) {
		case bool:
			val = v
		default:
			// For non-boolean types, select no rows
			return selection, 0
		}

		// Apply the comparison based on operator
		switch p.Operator {
		case "=", "==":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] == val {
					selection = append(selection, i)
				}
			}
		case "!=", "<>":
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && col[i] != val {
					selection = append(selection, i)
				}
			}
		}
	}

	return selection, len(selection)
}

// CompareColumnsProcessor compares two columns
type CompareColumnsProcessor struct {
	// The columns to compare
	LeftColumn  string
	RightColumn string

	// The comparison operator (=, <>, >, >=, <, <=)
	Operator string
}

// NewCompareColumnsProcessor creates a new column comparison processor
func NewCompareColumnsProcessor(leftCol, op, rightCol string) *CompareColumnsProcessor {
	return &CompareColumnsProcessor{
		LeftColumn:  leftCol,
		RightColumn: rightCol,
		Operator:    op,
	}
}

// getSelectionVector creates a selection vector of row indices that match the condition
func (p *CompareColumnsProcessor) getSelectionVector(batch *Batch) (SelectionVector, int) {
	// Get a selection vector from the pool
	selection := getSelectionVector(batch.Size)

	// Case 1: Both columns are integers
	if batch.HasIntColumn(p.LeftColumn) && batch.HasIntColumn(p.RightColumn) {
		leftCol := batch.IntColumns[p.LeftColumn]
		rightCol := batch.IntColumns[p.RightColumn]
		leftNulls := batch.NullBitmaps[p.LeftColumn]
		rightNulls := batch.NullBitmaps[p.RightColumn]

		for i := 0; i < batch.Size; i++ {
			// NULL handling
			if leftNulls[i] || rightNulls[i] {
				continue // NULLs don't match in comparisons
			}

			// Comparison
			switch p.Operator {
			case "=", "==":
				if leftCol[i] == rightCol[i] {
					selection = append(selection, i)
				}
			case "!=", "<>":
				if leftCol[i] != rightCol[i] {
					selection = append(selection, i)
				}
			case ">":
				if leftCol[i] > rightCol[i] {
					selection = append(selection, i)
				}
			case ">=":
				if leftCol[i] >= rightCol[i] {
					selection = append(selection, i)
				}
			case "<":
				if leftCol[i] < rightCol[i] {
					selection = append(selection, i)
				}
			case "<=":
				if leftCol[i] <= rightCol[i] {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasFloatColumn(p.LeftColumn) && batch.HasFloatColumn(p.RightColumn) {
		// Case 2: Both columns are floats
		leftCol := batch.FloatColumns[p.LeftColumn]
		rightCol := batch.FloatColumns[p.RightColumn]
		leftNulls := batch.NullBitmaps[p.LeftColumn]
		rightNulls := batch.NullBitmaps[p.RightColumn]

		for i := 0; i < batch.Size; i++ {
			// NULL handling
			if leftNulls[i] || rightNulls[i] {
				continue // NULLs don't match in comparisons
			}

			// Comparison
			switch p.Operator {
			case "=", "==":
				if leftCol[i] == rightCol[i] {
					selection = append(selection, i)
				}
			case "!=", "<>":
				if leftCol[i] != rightCol[i] {
					selection = append(selection, i)
				}
			case ">":
				if leftCol[i] > rightCol[i] {
					selection = append(selection, i)
				}
			case ">=":
				if leftCol[i] >= rightCol[i] {
					selection = append(selection, i)
				}
			case "<":
				if leftCol[i] < rightCol[i] {
					selection = append(selection, i)
				}
			case "<=":
				if leftCol[i] <= rightCol[i] {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasStringColumn(p.LeftColumn) && batch.HasStringColumn(p.RightColumn) {
		// Case 3: Both columns are strings
		leftCol := batch.StringColumns[p.LeftColumn]
		rightCol := batch.StringColumns[p.RightColumn]
		leftNulls := batch.NullBitmaps[p.LeftColumn]
		rightNulls := batch.NullBitmaps[p.RightColumn]

		for i := 0; i < batch.Size; i++ {
			// NULL handling
			if leftNulls[i] || rightNulls[i] {
				continue // NULLs don't match in comparisons
			}

			// Comparison
			switch p.Operator {
			case "=", "==":
				if leftCol[i] == rightCol[i] {
					selection = append(selection, i)
				}
			case "!=", "<>":
				if leftCol[i] != rightCol[i] {
					selection = append(selection, i)
				}
			case ">":
				if leftCol[i] > rightCol[i] {
					selection = append(selection, i)
				}
			case ">=":
				if leftCol[i] >= rightCol[i] {
					selection = append(selection, i)
				}
			case "<":
				if leftCol[i] < rightCol[i] {
					selection = append(selection, i)
				}
			case "<=":
				if leftCol[i] <= rightCol[i] {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasBoolColumn(p.LeftColumn) && batch.HasBoolColumn(p.RightColumn) {
		// Case 4: Both columns are booleans
		leftCol := batch.BoolColumns[p.LeftColumn]
		rightCol := batch.BoolColumns[p.RightColumn]
		leftNulls := batch.NullBitmaps[p.LeftColumn]
		rightNulls := batch.NullBitmaps[p.RightColumn]

		for i := 0; i < batch.Size; i++ {
			// NULL handling
			if leftNulls[i] || rightNulls[i] {
				continue // NULLs don't match in comparisons
			}

			// Comparison - booleans only support equality operators
			switch p.Operator {
			case "=", "==":
				if leftCol[i] == rightCol[i] {
					selection = append(selection, i)
				}
			case "!=", "<>":
				if leftCol[i] != rightCol[i] {
					selection = append(selection, i)
				}
			}
		}
	} else if batch.HasTimeColumn(p.LeftColumn) && batch.HasTimeColumn(p.RightColumn) {
		// Case 5: Both columns are time values
		leftCol := batch.TimeColumns[p.LeftColumn]
		rightCol := batch.TimeColumns[p.RightColumn]
		leftNulls := batch.NullBitmaps[p.LeftColumn]
		rightNulls := batch.NullBitmaps[p.RightColumn]

		for i := 0; i < batch.Size; i++ {
			// NULL handling
			if leftNulls[i] || rightNulls[i] {
				continue // NULLs don't match in comparisons
			}

			// Comparison
			switch p.Operator {
			case "=", "==":
				if leftCol[i].Equal(rightCol[i]) {
					selection = append(selection, i)
				}
			case "!=", "<>":
				if !leftCol[i].Equal(rightCol[i]) {
					selection = append(selection, i)
				}
			case ">":
				if leftCol[i].After(rightCol[i]) {
					selection = append(selection, i)
				}
			case ">=":
				if leftCol[i].After(rightCol[i]) || leftCol[i].Equal(rightCol[i]) {
					selection = append(selection, i)
				}
			case "<":
				if leftCol[i].Before(rightCol[i]) {
					selection = append(selection, i)
				}
			case "<=":
				if leftCol[i].Before(rightCol[i]) || leftCol[i].Equal(rightCol[i]) {
					selection = append(selection, i)
				}
			}
		}
	}

	return selection, len(selection)
}

// Process applies the comparison operation to the batch
func (p *CompareColumnsProcessor) Process(batch *Batch) (*Batch, error) {
	// Get a selection vector of which rows match
	selection, rowCount := p.getSelectionVector(batch)

	// Make sure to release the selection vector when we're done
	defer releaseSelectionVector(selection)

	// If no rows match, return an empty batch
	if rowCount == 0 {
		return NewBatch(0), nil
	}

	// Create output batch with only the selected rows
	output := NewBatch(rowCount)

	// Add all columns from the batch
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			output.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			output.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			output.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			output.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			output.AddTimeColumn(colName)
		}
	}

	// Copy only selected rows using the selection vector
	for outRow := 0; outRow < rowCount; outRow++ {
		srcRow := selection[outRow]

		// Copy values for this row
		for _, colName := range output.ColumnNames {
			if output.HasIntColumn(colName) {
				output.IntColumns[colName][outRow] = batch.IntColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasFloatColumn(colName) {
				output.FloatColumns[colName][outRow] = batch.FloatColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasStringColumn(colName) {
				output.StringColumns[colName][outRow] = batch.StringColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasBoolColumn(colName) {
				output.BoolColumns[colName][outRow] = batch.BoolColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			} else if output.HasTimeColumn(colName) {
				output.TimeColumns[colName][outRow] = batch.TimeColumns[colName][srcRow]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][srcRow]
			}
		}
	}

	return output, nil
}

// AndProcessor combines two processors with AND logic
type AndProcessor struct {
	Left  Processor
	Right Processor
}

// NewAndProcessor creates a new AND processor
func NewAndProcessor(left, right Processor) *AndProcessor {
	return &AndProcessor{
		Left:  left,
		Right: right,
	}
}

// Process applies both processors sequentially (AND)
func (p *AndProcessor) Process(batch *Batch) (*Batch, error) {
	// Apply left condition first
	intermediate, err := p.Left.Process(batch)
	if err != nil {
		return nil, err
	}

	// If empty result, return early
	if intermediate.Size == 0 {
		return intermediate, nil
	}

	// Apply right condition to the filtered result
	result, err := p.Right.Process(intermediate)

	// Release the intermediate batch since we're done with it
	ReleaseBatch(intermediate)

	return result, err
}

// OrProcessor combines two processors with OR logic
type OrProcessor struct {
	Left  Processor
	Right Processor
}

// NewOrProcessor creates a new OR processor
func NewOrProcessor(left, right Processor) *OrProcessor {
	return &OrProcessor{
		Left:  left,
		Right: right,
	}
}

// Process applies both processors and merges the results (OR)
func (p *OrProcessor) Process(batch *Batch) (*Batch, error) {
	// Create optimized processors directly if possible (fast path for simple cases)
	if compareLeft, okLeft := p.Left.(*CompareConstantProcessor); okLeft {
		if compareRight, okRight := p.Right.(*CompareConstantProcessor); okRight {
			// Fast path for two simple comparison conditions on the same column
			if compareLeft.ColumnName == compareRight.ColumnName {
				// Try to combine the conditions instead of processing separately
				if combined := tryOptimizedOr(compareLeft, compareRight, batch); combined != nil {
					return combined, nil
				}
			}
		}
	}

	// Get the selection vectors for left and right conditions separately
	// (We'll optimize by using lower-level access instead of Process())
	leftSel, leftErr := getProcessorSelectionVector(p.Left, batch)
	if leftErr != nil {
		return nil, leftErr
	}

	// If the left side matches everything, we can stop early
	if len(leftSel) == batch.Size {
		return p.Left.Process(batch)
	}

	// Process the right side
	rightSel, rightErr := getProcessorSelectionVector(p.Right, batch)
	if rightErr != nil {
		return nil, rightErr
	}

	// Combine selections for OR operation
	// We reuse the selection vectors for efficiency
	selectionVector := getSelectionVector(batch.Size)

	// Create a bitmap for fast lookups
	bitmap := make([]bool, batch.Size)

	// Mark left matches
	for _, idx := range leftSel {
		bitmap[idx] = true
		selectionVector = append(selectionVector, idx)
	}

	// Add right matches that aren't already included
	for _, idx := range rightSel {
		if !bitmap[idx] {
			selectionVector = append(selectionVector, idx)
		}
	}

	// Sort selection vector for cache-friendly access
	// (Only sort if it's worth the overhead)
	if len(selectionVector) > 100 {
		// Use a simpler counting sort since indices are limited to batch.Size
		sortedSelection := make([]int, len(selectionVector))
		counts := make([]int, batch.Size)

		// Count occurrences
		for _, idx := range selectionVector {
			counts[idx]++
		}

		// Compute running sum
		pos := 0
		for i := 0; i < batch.Size; i++ {
			count := counts[i]
			counts[i] = pos
			pos += count
		}

		// Place elements
		for _, idx := range selectionVector {
			sortedSelection[counts[idx]] = idx
			counts[idx]++
		}

		// Replace with sorted selection
		releaseSelectionVector(selectionVector)
		selectionVector = sortedSelection
	}

	// If no matches, return empty batch
	if len(selectionVector) == 0 {
		releaseSelectionVector(selectionVector)
		return NewBatch(0), nil
	}

	// Create output batch
	output := NewBatch(len(selectionVector))

	// Add all columns from the batch
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			output.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			output.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			output.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			output.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			output.AddTimeColumn(colName)
		}
	}

	// Copy selected rows in an optimized way
	for outIdx, srcIdx := range selectionVector {
		// Copy values for this row efficiently
		for _, colName := range output.ColumnNames {
			if output.HasIntColumn(colName) {
				output.IntColumns[colName][outIdx] = batch.IntColumns[colName][srcIdx]
				output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][srcIdx]
			} else if output.HasFloatColumn(colName) {
				output.FloatColumns[colName][outIdx] = batch.FloatColumns[colName][srcIdx]
				output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][srcIdx]
			} else if output.HasStringColumn(colName) {
				output.StringColumns[colName][outIdx] = batch.StringColumns[colName][srcIdx]
				output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][srcIdx]
			} else if output.HasBoolColumn(colName) {
				output.BoolColumns[colName][outIdx] = batch.BoolColumns[colName][srcIdx]
				output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][srcIdx]
			} else if output.HasTimeColumn(colName) {
				output.TimeColumns[colName][outIdx] = batch.TimeColumns[colName][srcIdx]
				output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][srcIdx]
			}
		}
	}

	// Release the selection vector
	releaseSelectionVector(selectionVector)

	return output, nil
}

// getProcessorSelectionVector gets a selection vector from a processor without creating a full batch
// This avoids the overhead of creating new batches for intermediate results
func getProcessorSelectionVector(proc Processor, batch *Batch) ([]int, error) {
	// Handle different processor types directly
	switch p := proc.(type) {
	case *CompareConstantProcessor:
		// Get selection vector directly without creating a new batch
		sel, count := p.getSelectionVector(batch)
		return sel[:count], nil

	case *CompareColumnsProcessor:
		// Get selection vector directly
		sel, count := p.getSelectionVector(batch)
		return sel[:count], nil

	case *SIMDComparisonProcessor:
		// Use optimized method for SIMD-accelerated comparisons
		sel, count := p.getSelectionVector(batch)
		return sel[:count], nil

	default:
		// For other processors, we need to use the standard Process method
		result, err := proc.Process(batch)
		if err != nil {
			return nil, err
		}

		// Convert the result batch to a selection vector
		selection := getSelectionVector(result.Size)

		// For the simple case of an empty result
		if result.Size == 0 {
			ReleaseBatch(result)
			return selection, nil
		}

		// We need to find the original indices in the source batch
		// For now, we'll use a simple but not optimal approach
		// TODO: Implement a more efficient mapping from result back to source
		for i := 0; i < batch.Size; i++ {
			// For each row in the source batch, check if it appears in the result
			found := false
			for j := 0; j < result.Size; j++ {
				matches := true

				// Check a sample of columns to see if this is the same row
				// We don't need to check all columns, just enough to uniquely identify
				sampleCols := min(3, len(batch.ColumnNames))
				for k := 0; k < sampleCols; k++ {
					colName := batch.ColumnNames[k]
					if batch.HasIntColumn(colName) && result.HasIntColumn(colName) {
						if batch.IntColumns[colName][i] != result.IntColumns[colName][j] {
							matches = false
							break
						}
					} else if batch.HasFloatColumn(colName) && result.HasFloatColumn(colName) {
						if batch.FloatColumns[colName][i] != result.FloatColumns[colName][j] {
							matches = false
							break
						}
					}
					// We only check numeric columns for efficiency
				}

				if matches {
					found = true
					break
				}
			}

			if found {
				selection = append(selection, i)
			}
		}

		// Release the result batch since we've extracted the selection
		ReleaseBatch(result)

		return selection, nil
	}
}

// tryOptimizedOr attempts to optimize OR conditions for simple cases
func tryOptimizedOr(left, right *CompareConstantProcessor, batch *Batch) *Batch {
	// Only handle the simple case of two constant comparisons on the same column and same type
	if left.ColumnName != right.ColumnName {
		return nil
	}

	// Check the types match
	leftType := getValueType(left.Value)
	rightType := getValueType(right.Value)
	if leftType != rightType {
		return nil
	}

	// For now, only handle integer equality tests (most common case)
	// This can be expanded to support more cases later
	if leftType == "int" &&
		(left.Operator == "=" || left.Operator == "==") &&
		(right.Operator == "=" || right.Operator == "==") {

		// Get the comparison values
		var leftVal, rightVal int64
		switch v := left.Value.(type) {
		case int64:
			leftVal = v
		case int:
			leftVal = int64(v)
		default:
			return nil
		}

		switch v := right.Value.(type) {
		case int64:
			rightVal = v
		case int:
			rightVal = int64(v)
		default:
			return nil
		}

		// Process directly with optimized code
		if batch.HasIntColumn(left.ColumnName) {
			// Get a quick count of matching rows
			matchCount := 0
			col := batch.IntColumns[left.ColumnName]
			nulls := batch.NullBitmaps[left.ColumnName]

			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && (col[i] == leftVal || col[i] == rightVal) {
					matchCount++
				}
			}

			// If no matches, return empty batch
			if matchCount == 0 {
				return NewBatch(0)
			}

			// Create output batch
			output := NewBatch(matchCount)

			// Add all columns from the batch
			for _, colName := range batch.ColumnNames {
				if batch.HasIntColumn(colName) {
					output.AddIntColumn(colName)
				} else if batch.HasFloatColumn(colName) {
					output.AddFloatColumn(colName)
				} else if batch.HasStringColumn(colName) {
					output.AddStringColumn(colName)
				} else if batch.HasBoolColumn(colName) {
					output.AddBoolColumn(colName)
				} else if batch.HasTimeColumn(colName) {
					output.AddTimeColumn(colName)
				}
			}

			// Copy matching rows
			outIdx := 0
			for i := 0; i < batch.Size; i++ {
				if !nulls[i] && (col[i] == leftVal || col[i] == rightVal) {
					// Copy all columns for this row
					for _, colName := range output.ColumnNames {
						if output.HasIntColumn(colName) {
							output.IntColumns[colName][outIdx] = batch.IntColumns[colName][i]
							output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][i]
						} else if output.HasFloatColumn(colName) {
							output.FloatColumns[colName][outIdx] = batch.FloatColumns[colName][i]
							output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][i]
						} else if output.HasStringColumn(colName) {
							output.StringColumns[colName][outIdx] = batch.StringColumns[colName][i]
							output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][i]
						} else if output.HasBoolColumn(colName) {
							output.BoolColumns[colName][outIdx] = batch.BoolColumns[colName][i]
							output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][i]
						} else if output.HasTimeColumn(colName) {
							output.TimeColumns[colName][outIdx] = batch.TimeColumns[colName][i]
							output.NullBitmaps[colName][outIdx] = batch.NullBitmaps[colName][i]
						}
					}
					outIdx++
				}
			}

			return output
		}
	}

	// Not optimizable with current implementation
	return nil
}

// getValueType returns the type of a value
func getValueType(val interface{}) string {
	switch val.(type) {
	case int, int64, int32:
		return "int"
	case float64, float32:
		return "float"
	case string:
		return "string"
	case bool:
		return "bool"
	default:
		return "unknown"
	}
}

// NotProcessor inverts the result of another processor
type NotProcessor struct {
	Inner Processor
}

// NewNotProcessor creates a new NOT processor
func NewNotProcessor(inner Processor) *NotProcessor {
	return &NotProcessor{
		Inner: inner,
	}
}

// Process applies the inner processor and inverts the result
func (p *NotProcessor) Process(batch *Batch) (*Batch, error) {
	// Apply inner condition to get the rows to exclude
	innerResult, err := p.Inner.Process(batch)
	if err != nil {
		return nil, err
	}

	// If inner result is empty, return all rows from the original batch
	if innerResult.Size == 0 {
		ReleaseBatch(innerResult)
		return NewPassthroughProcessor().Process(batch)
	}

	// If inner result includes all rows, return an empty result
	if innerResult.Size == batch.Size {
		ReleaseBatch(innerResult)
		return NewBatch(0), nil
	}

	// Create a selection vector where true means the row is NOT in the inner result
	selectionVector := make([]bool, batch.Size)
	for i := range selectionVector {
		selectionVector[i] = true // Start with all rows selected
	}

	// Mark rows that are in the inner result (to be excluded)
	innerIdx := 0
	for i := 0; i < batch.Size && innerIdx < innerResult.Size; i++ {
		// Check if this row is in the inner result
		isMatch := true

		// Check all columns to confirm this is the same row
		for _, colName := range batch.ColumnNames {
			if batch.HasIntColumn(colName) {
				if batch.IntColumns[colName][i] != innerResult.IntColumns[colName][innerIdx] {
					isMatch = false
					break
				}
			} else if batch.HasFloatColumn(colName) {
				if batch.FloatColumns[colName][i] != innerResult.FloatColumns[colName][innerIdx] {
					isMatch = false
					break
				}
			} else if batch.HasStringColumn(colName) {
				if batch.StringColumns[colName][i] != innerResult.StringColumns[colName][innerIdx] {
					isMatch = false
					break
				}
			} else if batch.HasBoolColumn(colName) {
				if batch.BoolColumns[colName][i] != innerResult.BoolColumns[colName][innerIdx] {
					isMatch = false
					break
				}
			} else if batch.HasTimeColumn(colName) {
				if !batch.TimeColumns[colName][i].Equal(innerResult.TimeColumns[colName][innerIdx]) {
					isMatch = false
					break
				}
			}
		}

		if isMatch {
			selectionVector[i] = false // Exclude this row
			innerIdx++

			// Look for the next matching row in the inner result
			if innerIdx < innerResult.Size {
				// Reset the search from the beginning
				i = -1 // Will be incremented to 0 by the loop
			}
		}
	}

	// Count how many rows to include
	rowCount := 0
	for i := 0; i < batch.Size; i++ {
		if selectionVector[i] {
			rowCount++
		}
	}

	// Create output batch with only the selected rows
	output := NewBatch(rowCount)

	// Add all columns from the batch
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			output.AddIntColumn(colName)
		} else if batch.HasFloatColumn(colName) {
			output.AddFloatColumn(colName)
		} else if batch.HasStringColumn(colName) {
			output.AddStringColumn(colName)
		} else if batch.HasBoolColumn(colName) {
			output.AddBoolColumn(colName)
		} else if batch.HasTimeColumn(colName) {
			output.AddTimeColumn(colName)
		}
	}

	// Copy only selected rows
	outRow := 0
	for i := 0; i < batch.Size; i++ {
		if !selectionVector[i] {
			continue
		}

		// Copy values for this row
		for _, colName := range output.ColumnNames {
			if output.HasIntColumn(colName) {
				output.IntColumns[colName][outRow] = batch.IntColumns[colName][i]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][i]
			} else if output.HasFloatColumn(colName) {
				output.FloatColumns[colName][outRow] = batch.FloatColumns[colName][i]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][i]
			} else if output.HasStringColumn(colName) {
				output.StringColumns[colName][outRow] = batch.StringColumns[colName][i]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][i]
			} else if output.HasBoolColumn(colName) {
				output.BoolColumns[colName][outRow] = batch.BoolColumns[colName][i]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][i]
			} else if output.HasTimeColumn(colName) {
				output.TimeColumns[colName][outRow] = batch.TimeColumns[colName][i]
				output.NullBitmaps[colName][outRow] = batch.NullBitmaps[colName][i]
			}
		}

		outRow++
	}

	// Release the inner result
	ReleaseBatch(innerResult)

	return output, nil
}

// PassthroughProcessor returns the batch unchanged
type PassthroughProcessor struct{}

// NewPassthroughProcessor creates a new passthrough processor
func NewPassthroughProcessor() *PassthroughProcessor {
	return &PassthroughProcessor{}
}

// Process returns the batch unchanged
func (p *PassthroughProcessor) Process(batch *Batch) (*Batch, error) {
	// Create a copy of the batch
	result := NewBatch(batch.Size)

	// Copy all columns
	for _, colName := range batch.ColumnNames {
		if batch.HasIntColumn(colName) {
			result.AddIntColumnFrom(colName, batch)
		} else if batch.HasFloatColumn(colName) {
			result.AddFloatColumnFrom(colName, batch)
		} else if batch.HasStringColumn(colName) {
			result.AddStringColumnFrom(colName, batch)
		} else if batch.HasBoolColumn(colName) {
			result.AddBoolColumnFrom(colName, batch)
		} else if batch.HasTimeColumn(colName) {
			result.AddTimeColumnFrom(colName, batch)
		}
	}

	return result, nil
}
