package expression

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/storage"
)

// SchemaAwareExpression wraps an Expression with schema information for efficient evaluation
// This allows expressions to work with column names instead of just positions in the row
type SchemaAwareExpression struct {
	Expr           storage.Expression // The original expression
	Schema         storage.Schema     // Schema information
	ColumnNames    []string           // Column names for the wrapped expression
	ColumnMap      map[string]int     // Pre-computed column index mapping
	DirectIndices  []int              // Direct array of column indices by position for fast lookups
	DirectColumns  []string           // Corresponding column names for the direct indices
	IndicesPrepped bool               // Flag indicating if direct indices are prepared
	aliases        map[string]string  // Column aliases (alias -> original column name)
}

// Pool for SchemaAwareExpression objects to reduce allocations
var schemaAwareExprPool = sync.Pool{
	New: func() interface{} {
		return &SchemaAwareExpression{
			ColumnNames: make([]string, 0, 16),    // Pre-allocate with small capacity
			ColumnMap:   make(map[string]int, 32), // Pre-allocate with small capacity
		}
	},
}

// NewSchemaAwareExpression creates an optimized expression with schema knowledge
func NewSchemaAwareExpression(expr storage.Expression, schema storage.Schema) *SchemaAwareExpression {
	// Get an expression from the pool
	saeInterface := schemaAwareExprPool.Get()
	sae, ok := saeInterface.(*SchemaAwareExpression)

	if !ok || sae == nil {
		// Fallback if something went wrong with the pool
		sae = &SchemaAwareExpression{
			ColumnNames:   make([]string, len(schema.Columns)),
			ColumnMap:     make(map[string]int, len(schema.Columns)),
			DirectIndices: make([]int, len(schema.Columns)),
			DirectColumns: make([]string, len(schema.Columns)),
		}
	} else {
		// Reset and resize the slices
		if cap(sae.ColumnNames) >= len(schema.Columns) {
			sae.ColumnNames = sae.ColumnNames[:len(schema.Columns)]
		} else {
			sae.ColumnNames = make([]string, len(schema.Columns))
		}

		if cap(sae.DirectIndices) >= len(schema.Columns) {
			sae.DirectIndices = sae.DirectIndices[:len(schema.Columns)]
		} else {
			sae.DirectIndices = make([]int, len(schema.Columns))
		}

		if cap(sae.DirectColumns) >= len(schema.Columns) {
			sae.DirectColumns = sae.DirectColumns[:len(schema.Columns)]
		} else {
			sae.DirectColumns = make([]string, len(schema.Columns))
		}

		// Clear the map
		clear(sae.ColumnMap)
	}

	// Set fields
	sae.Expr = expr
	sae.Schema = schema
	sae.aliases = nil         // Reset any aliases
	sae.IndicesPrepped = true // Mark indices as prepared

	// Pre-compute column mappings (single allocation for the map)
	for i, col := range schema.Columns {
		sae.ColumnNames[i] = col.Name
		sae.ColumnMap[col.Name] = i // Map column name to its index

		// Also populate direct indices for faster lookups
		sae.DirectIndices[i] = i
		sae.DirectColumns[i] = col.Name

		// Also add lowercase version of column name for case-insensitive lookups
		lowerName := strings.ToLower(col.Name)
		if _, exists := sae.ColumnMap[lowerName]; !exists {
			sae.ColumnMap[lowerName] = i
		}
	}

	return sae
}

// ReturnToPool returns a SchemaAwareExpression to the pool
func ReturnSchemaAwereExpressionPool(sae *SchemaAwareExpression) {
	if sae == nil {
		return
	}

	// Clear references to allow garbage collection
	sae.Expr = nil
	sae.Schema = storage.Schema{}
	sae.aliases = nil
	sae.IndicesPrepped = false

	// Return to the pool
	schemaAwareExprPool.Put(sae)
}

// PrepareDirectIndices initializes the direct indices for faster column lookups
func (sae *SchemaAwareExpression) PrepareDirectIndices() {
	if sae.IndicesPrepped {
		return
	}

	// Initialize direct indices array if needed
	if sae.DirectIndices == nil || len(sae.DirectIndices) != len(sae.Schema.Columns) {
		sae.DirectIndices = make([]int, len(sae.Schema.Columns))
		sae.DirectColumns = make([]string, len(sae.Schema.Columns))
	}

	// Populate direct indices lookup
	for i, col := range sae.Schema.Columns {
		sae.DirectIndices[i] = i
		sae.DirectColumns[i] = col.Name
	}

	sae.IndicesPrepped = true
}

// FindColumnIndex finds a column index using the fastest lookup method available
func (sae *SchemaAwareExpression) FindColumnIndex(columnName string) int {
	// Ensure direct indices are prepared
	if !sae.IndicesPrepped {
		sae.PrepareDirectIndices()
	}

	// Try direct lookup first (fastest)
	for i, col := range sae.DirectColumns {
		if col == columnName {
			return sae.DirectIndices[i]
		}
	}

	// Fall back to map lookup (slower but handles aliases)
	if idx, ok := sae.ColumnMap[columnName]; ok {
		return idx
	}

	// Try case-insensitive lookup as final fallback
	if idx, ok := sae.ColumnMap[strings.ToLower(columnName)]; ok {
		return idx
	}

	return -1 // Column not found
}

// Evaluate implements the Expression interface with schema awareness
// andExprCache is a lightweight wrapper for caching schema-aware expressions for AND operations
type andExprCache struct {
	expr      *AndExpression
	schemaID  string // Using schema name as ID instead of the full schema
	children  []storage.Expression
	evaluated bool
}

// orExprCache is a lightweight wrapper for caching schema-aware expressions for OR operations
type orExprCache struct {
	expr      *OrExpression
	schemaID  string // Using schema name as ID instead of the full schema
	children  []storage.Expression
	evaluated bool
}

// Cache for logical expression evaluations to reduce allocations
var (
	andCachePool = sync.Pool{
		New: func() interface{} {
			return &andExprCache{
				children: make([]storage.Expression, 0, 8),
			}
		},
	}
	orCachePool = sync.Pool{
		New: func() interface{} {
			return &orExprCache{
				children: make([]storage.Expression, 0, 8),
			}
		},
	}
)

func (sae *SchemaAwareExpression) Evaluate(row storage.Row) (bool, error) {
	if sae == nil || sae.Expr == nil {
		return false, nil
	}

	// Fast path for SimpleExpression - most common case
	if simpleExpr, ok := sae.Expr.(*SimpleExpression); ok {
		if !simpleExpr.IndexPrepped {
			// Attempt direct lookup first
			for i := 0; i < len(sae.DirectColumns); i++ {
				if sae.DirectColumns[i] == simpleExpr.Column {
					simpleExpr.ColIndex = sae.DirectIndices[i]
					simpleExpr.IndexPrepped = true
					break
				}
			}

			// Fallback to map lookup if not found
			if !simpleExpr.IndexPrepped {
				if idx, ok := sae.ColumnMap[simpleExpr.Column]; ok {
					simpleExpr.ColIndex = idx
					simpleExpr.IndexPrepped = true
				} else if idx, ok := sae.ColumnMap[strings.ToLower(simpleExpr.Column)]; ok {
					simpleExpr.ColIndex = idx
					simpleExpr.IndexPrepped = true
				}
			}
		}

		if simpleExpr.IndexPrepped {
			return simpleExpr.Evaluate(row)
		}
	}

	// Handle different expression types
	switch expr := sae.Expr.(type) {
	case *NullCheckExpression:
		// Use the optimized direct lookup method for column index
		colIndex := sae.FindColumnIndex(expr.GetColumnName())

		// If column not found or out of range, default to treating as NULL
		if colIndex == -1 || colIndex >= len(row) {
			return expr.isNull, nil
		}

		// Get the column value
		colValue := row[colIndex]

		// Check if value is NULL
		isNull := colValue == nil || colValue.IsNull()

		// For IS NULL, return true if value is NULL
		// For IS NOT NULL, return true if value is NOT NULL
		if expr.isNull {
			return isNull, nil
		} else {
			return !isNull, nil
		}

	case *BetweenExpression:
		// Use the optimized direct lookup method
		colIndex := sae.FindColumnIndex(expr.Column)

		// Column not found in schema
		if colIndex == -1 || colIndex >= len(row) {
			return false, nil
		}

		// Get the column value
		colValue := row[colIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL BETWEEN ... is always false
			return false, nil
		}

		// Try to use the appropriate type comparison based on column type
		switch colValue.Type() {
		case storage.INTEGER:
			// For integers, convert and compare numerically
			colInt, ok := colValue.AsInt64()
			if !ok {
				return false, nil
			}

			// Convert lower bound to int64
			var lowerInt int64
			switch v := expr.LowerBound.(type) {
			case int:
				lowerInt = int64(v)
			case int64:
				lowerInt = v
			case float64:
				lowerInt = int64(v)
			case string:
				// Try to parse as int
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
					lowerInt = parsed
				} else {
					return false, fmt.Errorf("cannot convert lower bound to int64: %s", v)
				}
			default:
				return false, fmt.Errorf("unsupported type for lower bound: %T", expr.LowerBound)
			}

			// Convert upper bound to int64
			var upperInt int64
			switch v := expr.UpperBound.(type) {
			case int:
				upperInt = int64(v)
			case int64:
				upperInt = v
			case float64:
				upperInt = int64(v)
			case string:
				// Try to parse as int
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
					upperInt = parsed
				} else {
					return false, fmt.Errorf("cannot convert upper bound to int64: %s", v)
				}
			default:
				return false, fmt.Errorf("unsupported type for upper bound: %T", expr.UpperBound)
			}

			// Compare with bounds
			if expr.Inclusive {
				result := colInt >= lowerInt && colInt <= upperInt
				return result, nil
			}
			return colInt > lowerInt && colInt < upperInt, nil

		case storage.FLOAT:
			// For floats, convert and compare numerically
			colFloat, ok := colValue.AsFloat64()
			if !ok {
				return false, nil
			}

			// Convert lower bound to float64
			var lowerFloat float64
			switch v := expr.LowerBound.(type) {
			case int:
				lowerFloat = float64(v)
			case int64:
				lowerFloat = float64(v)
			case float64:
				lowerFloat = v
			case string:
				// Try to parse as float
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					lowerFloat = parsed
				} else {
					return false, fmt.Errorf("cannot convert lower bound to float64: %s", v)
				}
			default:
				return false, fmt.Errorf("unsupported type for lower bound: %T", expr.LowerBound)
			}

			// Convert upper bound to float64
			var upperFloat float64
			switch v := expr.UpperBound.(type) {
			case int:
				upperFloat = float64(v)
			case int64:
				upperFloat = float64(v)
			case float64:
				upperFloat = v
			case string:
				// Try to parse as float
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					upperFloat = parsed
				} else {
					return false, fmt.Errorf("cannot convert upper bound to float64: %s", v)
				}
			default:
				return false, fmt.Errorf("unsupported type for upper bound: %T", expr.UpperBound)
			}

			// Compare with bounds
			if expr.Inclusive {
				return colFloat >= lowerFloat && colFloat <= upperFloat, nil
			}
			return colFloat > lowerFloat && colFloat < upperFloat, nil

		default:
			// For other types, fall back to string comparison
			colStrVal, ok := colValue.AsString()
			if !ok {
				return false, nil
			}

			// Convert lower bound to string
			var lowerStr string
			switch v := expr.LowerBound.(type) {
			case string:
				lowerStr = v
			default:
				lowerStr = fmt.Sprintf("%v", expr.LowerBound)
			}

			// Convert upper bound to string
			var upperStr string
			switch v := expr.UpperBound.(type) {
			case string:
				upperStr = v
			default:
				upperStr = fmt.Sprintf("%v", expr.UpperBound)
			}

			// Compare with bounds
			if expr.Inclusive {
				return colStrVal >= lowerStr && colStrVal <= upperStr, nil
			}
			return colStrVal > lowerStr && colStrVal < upperStr, nil
		}
	case *InListExpression:
		// Use the optimized direct lookup method
		colIndex := sae.FindColumnIndex(expr.Column)

		// Column not found in schema
		if colIndex == -1 || colIndex >= len(row) {
			return false, nil
		}

		// Get the column value
		colValue := row[colIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL IN (...) is always false
			// For NOT IN this remains false (NULL NOT IN ... is also false)
			return false, nil
		}

		// Try to use the appropriate type comparison based on column type
		switch colValue.Type() {
		case storage.INTEGER:
			// For integers, convert and compare numerically
			colInt, ok := colValue.AsInt64()
			if !ok {
				return false, nil
			}

			// Check if the value is in the list
			for _, value := range expr.Values {
				// Convert value to int64 for comparison
				var valueInt int64
				switch v := value.(type) {
				case int:
					valueInt = int64(v)
				case int64:
					valueInt = v
				case float64:
					valueInt = int64(v)
				case string:
					// Try to parse as int
					if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
						valueInt = parsed
					} else {
						// Skip this value if not parseable
						continue
					}
				default:
					// Skip non-numeric values
					continue
				}

				// Compare
				if colInt == valueInt {
					return true, nil
				}
			}

		case storage.FLOAT:
			// For floats, convert and compare numerically
			colFloat, ok := colValue.AsFloat64()
			if !ok {
				return false, nil
			}

			// Check if the value is in the list
			for _, value := range expr.Values {
				// Convert value to float64 for comparison
				var valueFloat float64
				switch v := value.(type) {
				case int:
					valueFloat = float64(v)
				case int64:
					valueFloat = float64(v)
				case float64:
					valueFloat = v
				case string:
					// Try to parse as float
					if parsed, err := strconv.ParseFloat(v, 64); err == nil {
						valueFloat = parsed
					} else {
						// Skip this value if not parseable
						continue
					}
				default:
					// Skip non-numeric values
					continue
				}

				// Compare
				if colFloat == valueFloat {
					return true, nil
				}
			}

		default:
			// For other types, fall back to string comparison
			colStrVal, ok := colValue.AsString()
			if !ok {
				return false, nil
			}

			// Check if the value is in the list
			for _, value := range expr.Values {
				// Convert value to string for comparison
				var valueStr string
				switch v := value.(type) {
				case string:
					valueStr = v
				default:
					valueStr = fmt.Sprintf("%v", v)
				}

				// Compare
				if colStrVal == valueStr {
					// For NOT IN, return false immediately if we find a match
					if expr.Not {
						return false, nil
					}
					return true, nil
				}
			}
		}

		// If we didn't find a match
		// For NOT IN, that means all values are NOT IN the list, so return true
		if expr.Not {
			return true, nil
		}
		return false, nil
	case *FastSimpleExpression:
		expr.PrepareForSchema(sae.Schema)
		return expr.Evaluate(row)
	case *SimpleExpression:
		// Ultra-fast path: try to use the cached column index if already prepped
		if expr.IndexPrepped && expr.ColIndex >= 0 && expr.ColIndex < len(row) {
			// Fast path - use the high-performance evaluation
			return expr.Evaluate(row)
		}

		// Fast path preparation: use direct index lookup (no string comparisons)
		if !expr.IndexPrepped {
			// Use our new optimized column lookup
			colIndex := sae.FindColumnIndex(expr.Column)

			if colIndex >= 0 {
				expr.ColIndex = colIndex
				expr.IndexPrepped = true

				if colIndex < len(row) {
					// We found the column - use the fast path immediately
					return expr.Evaluate(row)
				}
			}
		}

		// Fallback to slice index lookup (still faster than map for small columns)
		colIndex := -1
		if len(sae.DirectColumns) > 0 && sae.IndicesPrepped {
			// Use direct arrays which are faster than slice.Index
			for i, col := range sae.DirectColumns {
				if col == expr.Column {
					colIndex = sae.DirectIndices[i]
					break
				}
			}
		} else {
			colIndex = slices.Index(sae.ColumnNames, expr.Column)
		}

		if colIndex == -1 {
			// Try case-insensitive lookup as last resort
			var ok bool
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.Column)]
			if !ok {
				colIndex = -1
			}
		}

		if colIndex >= 0 && colIndex < len(row) {
			// Cache the column index for future evaluations
			expr.ColIndex = colIndex
			expr.IndexPrepped = true

			// We found the column - now evaluate the condition using the optimized path
			return expr.Evaluate(row)
		} else {
			return false, fmt.Errorf("column '%s' not found in row", expr.Column)
		}

	case *AndExpression:
		// Use cached schema expressions when available
		if expr.cachedSchemaExpressions != nil {
			// Use the already cached expressions
			for _, childExpr := range expr.cachedSchemaExpressions {
				result, err := childExpr.Evaluate(row)
				if err != nil {
					return false, err
				}
				if !result {
					return false, nil // Short-circuit AND on first false
				}
			}
			return true, nil
		}

		// Get a cache object from the pool
		cacheObj := andCachePool.Get().(*andExprCache)
		schemaID := sae.Schema.TableName // Use schema name as unique identifier

		if cacheObj.expr != expr || !cacheObj.evaluated || cacheObj.schemaID != schemaID {
			// Reset the cache for this expression
			cacheObj.expr = expr
			cacheObj.schemaID = schemaID
			cacheObj.evaluated = true

			// Make sure we have enough capacity
			if cap(cacheObj.children) < len(expr.Expressions) {
				cacheObj.children = make([]storage.Expression, 0, len(expr.Expressions))
			} else {
				cacheObj.children = cacheObj.children[:0]
			}

			// Create schema expressions for each child
			for _, childExpr := range expr.Expressions {
				// Only create new schema-aware wrappers if needed
				if saExpr, ok := childExpr.(*SchemaAwareExpression); ok {
					cacheObj.children = append(cacheObj.children, saExpr)
				} else if simpleExpr, ok := childExpr.(*SimpleExpression); ok {
					if !simpleExpr.IndexPrepped {
						simpleExpr.PrepareForSchema(sae.Schema)
					}
					cacheObj.children = append(cacheObj.children, simpleExpr)
				} else {
					cacheObj.children = append(cacheObj.children, NewSchemaAwareExpression(childExpr, sae.Schema))
				}
			}

			// Store in the expression for future use
			expr.cachedSchemaExpressions = cacheObj.children
		}

		// Evaluate using the cached children
		for _, childExpr := range expr.cachedSchemaExpressions {
			result, err := childExpr.Evaluate(row)
			if err != nil {
				andCachePool.Put(cacheObj)
				return false, err
			}
			if !result {
				andCachePool.Put(cacheObj)
				return false, nil // Short-circuit AND on first false
			}
		}

		andCachePool.Put(cacheObj)
		return true, nil

	case *OrExpression:
		// Use cached schema expressions when available
		if expr.cachedSchemaExpressions != nil {
			// Use the already cached expressions
			for _, childExpr := range expr.cachedSchemaExpressions {
				result, err := childExpr.Evaluate(row)
				if err != nil {
					continue // For OR, we can continue even if one child fails
				}
				if result {
					return true, nil // Short-circuit OR on first true
				}
			}
			return false, nil
		}

		// Get a cache object from the pool
		cacheObj := orCachePool.Get().(*orExprCache)
		schemaID := sae.Schema.TableName // Use schema name as unique identifier

		if cacheObj.expr != expr || !cacheObj.evaluated || cacheObj.schemaID != schemaID {
			// Reset the cache for this expression
			cacheObj.expr = expr
			cacheObj.schemaID = schemaID
			cacheObj.evaluated = true

			// Make sure we have enough capacity
			if cap(cacheObj.children) < len(expr.Expressions) {
				cacheObj.children = make([]storage.Expression, 0, len(expr.Expressions))
			} else {
				cacheObj.children = cacheObj.children[:0]
			}

			// Create schema expressions for each child
			for _, childExpr := range expr.Expressions {
				// Only create new schema-aware wrappers if needed
				if saExpr, ok := childExpr.(*SchemaAwareExpression); ok {
					cacheObj.children = append(cacheObj.children, saExpr)
				} else if simpleExpr, ok := childExpr.(*SimpleExpression); ok {
					if !simpleExpr.IndexPrepped {
						simpleExpr.PrepareForSchema(sae.Schema)
					}
					cacheObj.children = append(cacheObj.children, simpleExpr)
				} else {
					cacheObj.children = append(cacheObj.children, NewSchemaAwareExpression(childExpr, sae.Schema))
				}
			}

			// Store in the expression for future use
			expr.cachedSchemaExpressions = cacheObj.children
		}

		// Evaluate using the cached children
		for _, childExpr := range expr.cachedSchemaExpressions {
			result, err := childExpr.Evaluate(row)
			if err != nil {
				continue // For OR, we can continue even if one child fails
			}
			if result {
				orCachePool.Put(cacheObj)
				return true, nil // Short-circuit OR on first true
			}
		}

		orCachePool.Put(cacheObj)
		return false, nil

	case *EvalExpression:
		// For EvalExpression, simply delegate to the original implementation
		// This preserves existing behavior and works with tests that rely on specific
		// row structure
		return expr.Evaluate(row)

	case *CastExpression:
		// Use the optimized direct lookup method for column index
		colIndex := sae.FindColumnIndex(expr.Column)

		// Column not found in schema
		if colIndex == -1 || colIndex >= len(row) {
			return false, fmt.Errorf("column '%s' not found in row", expr.Column)
		}

		// Get the column value
		colValue := row[colIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL values remain NULL after CAST
			return false, nil
		}

		// Perform the cast operation
		_, err := expr.PerformCast(colValue)
		if err != nil {
			return false, err
		}

		// CAST expression by itself should not filter rows
		// When used in a comparison, the parent expression will handle the comparison
		return true, nil

	default:
		// For other expression types, delegate to the original expression
		result, err := sae.Expr.Evaluate(row)
		if err != nil {
			return false, err
		}
		return result, nil
	}
}

// WithAliases implements the storage.Expression interface
func (sae *SchemaAwareExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Apply aliases to the wrapped expression if it supports them
	var newExpr storage.Expression

	// First apply aliases to the wrapped expression
	if aliasable, ok := sae.Expr.(interface {
		WithAliases(map[string]string) storage.Expression
	}); ok {
		newExpr = aliasable.WithAliases(aliases)
	} else {
		// If the inner expression doesn't support aliases, use the original
		newExpr = sae.Expr
	}

	// Create a new SchemaAwareExpression with the aliased inner expression
	result := &SchemaAwareExpression{
		Expr:           newExpr,
		Schema:         sae.Schema,
		ColumnNames:    make([]string, len(sae.ColumnNames)),
		ColumnMap:      make(map[string]int, len(sae.ColumnMap)),
		DirectIndices:  make([]int, len(sae.ColumnNames)),
		DirectColumns:  make([]string, len(sae.ColumnNames)),
		IndicesPrepped: true,
		aliases:        aliases,
	}

	// Copy the column names and direct indices
	copy(result.ColumnNames, sae.ColumnNames)
	copy(result.DirectIndices, sae.DirectIndices)
	copy(result.DirectColumns, sae.DirectColumns)

	// Copy the column mapping
	for k, v := range sae.ColumnMap {
		result.ColumnMap[k] = v
	}

	return result
}

// WithSchema implements the storage.SchemaAwareExpression interface
func (sae *SchemaAwareExpression) WithSchema(columnMap map[string]int) storage.Expression {
	// Create a new SchemaAwareExpression with updated column mapping
	result := &SchemaAwareExpression{
		Expr:           sae.Expr,
		Schema:         sae.Schema,
		ColumnNames:    make([]string, len(sae.ColumnNames)),
		ColumnMap:      make(map[string]int, len(columnMap)),
		DirectIndices:  make([]int, len(sae.ColumnNames)),
		DirectColumns:  make([]string, len(sae.ColumnNames)),
		aliases:        sae.aliases,
		IndicesPrepped: false, // Need to rebuild indices based on new columnMap
	}

	// Copy the column names
	copy(result.ColumnNames, sae.ColumnNames)

	// Use the provided column mapping
	for k, v := range columnMap {
		result.ColumnMap[k] = v
	}

	// Prepare direct indices based on the new column mapping
	if len(result.ColumnNames) > 0 {
		for i, colName := range result.ColumnNames {
			if idx, ok := columnMap[colName]; ok {
				result.DirectIndices[i] = idx
				result.DirectColumns[i] = colName
			} else {
				// Default to position-based index if not in map
				result.DirectIndices[i] = i
				result.DirectColumns[i] = colName
			}
		}
		result.IndicesPrepped = true
	}

	return result
}
