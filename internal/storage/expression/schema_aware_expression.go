package expression

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// SchemaAwareExpression wraps an Expression with schema information for efficient evaluation
// This allows expressions to work with column names instead of just positions in the row
type SchemaAwareExpression struct {
	Expr        storage.Expression // The original expression
	Schema      storage.Schema     // Schema information
	ColumnNames []string           // Column names for the wrapped expression
	ColumnMap   map[string]int     // Pre-computed column index mapping
	aliases     map[string]string  // Column aliases (alias -> original column name)
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
			ColumnNames: make([]string, len(schema.Columns)),
			ColumnMap:   make(map[string]int, len(schema.Columns)),
		}
	} else {
		// Reset and resize the string slice
		if cap(sae.ColumnNames) >= len(schema.Columns) {
			sae.ColumnNames = sae.ColumnNames[:len(schema.Columns)]
		} else {
			sae.ColumnNames = make([]string, len(schema.Columns))
		}

		// Clear the map
		clear(sae.ColumnMap)
	}

	// Set fields
	sae.Expr = expr
	sae.Schema = schema
	sae.aliases = nil // Reset any aliases

	// Pre-compute column mappings (single allocation for the map)
	for i, col := range schema.Columns {
		sae.ColumnNames[i] = col.Name
		sae.ColumnMap[col.Name] = i // Map column name to its index
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

	// Return to the pool
	schemaAwareExprPool.Put(sae)
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

	// Handle different expression types
	switch expr := sae.Expr.(type) {
	case *NullCheckExpression:
		// Find the column index by name using the map
		colIndex, ok := sae.ColumnMap[expr.GetColumnName()]
		if !ok {
			// Try lowercase version for case-insensitive match
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.GetColumnName())]
			if !ok {
				// Column not found in schema, set to -1 as fallback
				colIndex = -1
			}
		}

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
		// Find the column index by name using the map
		colIndex, ok := sae.ColumnMap[expr.Column]
		if !ok {
			// Try lowercase version for case-insensitive match
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.Column)]
			if !ok {
				// Column not found in schema, set to -1 as fallback
				colIndex = -1
			}
		}

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
		case storage.TypeInteger:
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

		case storage.TypeFloat:
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
		// Find the column index by name using the map
		colIndex, ok := sae.ColumnMap[expr.Column]
		if !ok {
			// Try case-insensitive lookup
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.Column)]
			if !ok {
				// Column not found in schema, set to -1 as fallback
				colIndex = -1
			}
		}

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
		case storage.TypeInteger:
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

		case storage.TypeFloat:
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
	case *SimpleExpression:
		// Find column index by name using the direct map lookup
		colIndex, ok := sae.ColumnMap[expr.Column]
		if !ok {
			// Try case-insensitive lookup
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.Column)]
			if !ok {
				// Column not found in schema, set to -1 as fallback
				colIndex = -1
			}
		}

		if colIndex >= 0 && colIndex < len(row) {
			// We found the column - now evaluate the condition
			colValue := row[colIndex]
			if colValue == nil || colValue.IsNull() {
				// Handle NULL values - properly check both nil and IsNull() interface
				return evaluateNull(expr.Operator)
			}

			result, err := evaluateComparison(colValue, expr.Operator, expr.Value)
			if err != nil {
				return false, err
			}
			return result, nil
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

	case *FunctionExpression:
		// For FunctionExpression, simply delegate to the original implementation
		// This preserves existing behavior and works with tests that rely on specific
		// row structure
		return expr.Evaluate(row)

	case *CastExpression:
		// Find the column index by name using direct map lookup
		colIndex, ok := sae.ColumnMap[expr.Column]
		if !ok {
			// Try case-insensitive lookup
			colIndex, ok = sae.ColumnMap[strings.ToLower(expr.Column)]
			if !ok {
				// Column not found in schema, set to -1 as fallback
				colIndex = -1
			}
		}

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

// evaluateNull handles comparisons against NULL values
func evaluateNull(operator storage.Operator) (bool, error) {
	switch operator {
	case storage.ISNULL:
		return true, nil
	case storage.ISNOTNULL:
		return false, nil
	default:
		// NULL comparisons with other operators return false
		return false, nil
	}
}

// evaluateComparison evaluates a comparison between a column value and expression value
func evaluateComparison(colValue storage.ColumnValue, operator storage.Operator, value interface{}) (bool, error) {
	// Switch on the column type
	switch colValue.Type() {
	case storage.TypeInteger: // TypeInteger and INTEGER are the same constant
		v, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Convert value to int64 for comparison
		var compareValue int64
		switch val := value.(type) {
		case int:
			compareValue = int64(val)
		case int64:
			compareValue = val
		case float64:
			compareValue = int64(val)
		default:
			// Special handling for mvcc.IntegerValue and other custom types
			if intVal, ok := value.(interface{ AsInt64() (int64, bool) }); ok {
				if i64, ok := intVal.AsInt64(); ok {
					compareValue = i64
				} else {
					return false, fmt.Errorf("failed to convert %T to int64", value)
				}
			} else {
				return false, fmt.Errorf("cannot compare integer with %T", value)
			}
		}

		// Apply the operator
		switch operator {
		case storage.EQ:
			result := v == compareValue
			return result, nil
		case storage.NE:
			result := v != compareValue
			return result, nil
		case storage.GT:
			result := v > compareValue
			return result, nil
		case storage.GTE:
			result := v >= compareValue
			return result, nil
		case storage.LT:
			result := v < compareValue
			return result, nil
		case storage.LTE:
			result := v <= compareValue
			return result, nil
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", operator)
		}

	case storage.TypeFloat: // TypeFloat and FLOAT are the same constant
		v, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Convert value to float64 for comparison
		var compareValue float64
		switch val := value.(type) {
		case int:
			compareValue = float64(val)
		case int64:
			compareValue = float64(val)
		case float64:
			compareValue = val
		default:
			// Special handling for mvcc.FloatValue and other custom types
			if floatVal, ok := value.(interface{ AsFloat64() (float64, bool) }); ok {
				if f64, ok := floatVal.AsFloat64(); ok {
					compareValue = f64
				} else {
					return false, fmt.Errorf("failed to convert %T to float64", value)
				}
			} else {
				return false, fmt.Errorf("cannot compare float with %T", value)
			}
		}

		// Apply the operator
		switch operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for float: %v", operator)
		}

	case storage.TypeString: // TypeString and TEXT are the same constant
		v, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Convert value to string for comparison
		var compareValue string
		switch val := value.(type) {
		case string:
			compareValue = val
		default:
			// Special handling for mvcc.StringValue and other custom types
			if strVal, ok := value.(interface{ AsString() (string, bool) }); ok {
				if str, ok := strVal.AsString(); ok {
					compareValue = str
				} else {
					compareValue = fmt.Sprintf("%v", value)
				}
			} else {
				compareValue = fmt.Sprintf("%v", val)
			}
		}

		// Apply the operator
		switch operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		case storage.LIKE:
			// LIKE pattern matching would need more implementation
			return false, fmt.Errorf("LIKE operator not implemented yet")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", operator)
		}

	case storage.TypeBoolean: // TypeBoolean and BOOLEAN are the same constant
		v, ok := colValue.AsBoolean()
		if !ok {
			return false, nil
		}

		// Convert value to boolean for comparison
		var compareValue bool
		switch val := value.(type) {
		case bool:
			compareValue = val
		case string:
			compareValue = val == "true" || val == "1"
		case int:
			compareValue = val != 0
		case int64:
			compareValue = val != 0
		default:
			// Special handling for mvcc.BooleanValue and other custom types
			if boolVal, ok := value.(interface{ AsBoolean() (bool, bool) }); ok {
				if b, ok := boolVal.AsBoolean(); ok {
					compareValue = b
				} else {
					return false, fmt.Errorf("failed to convert %T to boolean", value)
				}
			} else {
				return false, fmt.Errorf("cannot compare boolean with %T", value)
			}
		}

		// Apply the operator
		switch operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", operator)
		}

		// Handle date and timestamp types
	case storage.TypeDate:
		// For dates, we need special handling
		dateTime, ok := colValue.AsDate()
		if !ok {
			// Try timestamp as fallback
			dateTime, ok = colValue.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		// Convert value to time.Time for comparison
		var compareTime time.Time
		switch val := value.(type) {
		case time.Time:
			compareTime = val
		case string:
			var err error
			compareTime, err = storage.ParseDate(val)
			if err != nil {
				return false, fmt.Errorf("could not parse date string: %v", err)
			}
		default:
			return false, fmt.Errorf("cannot compare date with %T", value)
		}

		// For date comparison, we only care about the date part
		// Strip out the time components
		dt1 := time.Date(dateTime.Year(), dateTime.Month(), dateTime.Day(), 0, 0, 0, 0, time.UTC)
		dt2 := time.Date(compareTime.Year(), compareTime.Month(), compareTime.Day(), 0, 0, 0, 0, time.UTC)

		// Apply the operator to the dates
		switch operator {
		case storage.EQ:
			return dt1.Equal(dt2), nil
		case storage.NE:
			return !dt1.Equal(dt2), nil
		case storage.GT:
			return dt1.After(dt2), nil
		case storage.GTE:
			return dt1.After(dt2) || dt1.Equal(dt2), nil
		case storage.LT:
			return dt1.Before(dt2), nil
		case storage.LTE:
			return dt1.Before(dt2) || dt1.Equal(dt2), nil
		default:
			return false, fmt.Errorf("unsupported operator for date: %v", operator)
		}

	case storage.TypeTimestamp:
		// For timestamps, use a similar approach to date but with full precision
		timestamp, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Convert value to time.Time for comparison
		var compareTime time.Time
		switch val := value.(type) {
		case time.Time:
			compareTime = val
		case string:
			var err error
			compareTime, err = storage.ParseTimestamp(val)
			if err != nil {
				return false, fmt.Errorf("could not parse timestamp string: %v", err)
			}
		default:
			return false, fmt.Errorf("cannot compare timestamp with %T", value)
		}

		// Apply the operator to the timestamps
		switch operator {
		case storage.EQ:
			return timestamp.Equal(compareTime), nil
		case storage.NE:
			return !timestamp.Equal(compareTime), nil
		case storage.GT:
			return timestamp.After(compareTime), nil
		case storage.GTE:
			return timestamp.After(compareTime) || timestamp.Equal(compareTime), nil
		case storage.LT:
			return timestamp.Before(compareTime), nil
		case storage.LTE:
			return timestamp.Before(compareTime) || timestamp.Equal(compareTime), nil
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", operator)
		}

	case storage.TypeTime:
		// For time values, first try AsTime then fall back to AsTimestamp
		timeVal, ok := colValue.AsTime()
		if !ok {
			// Fall back to AsTimestamp for backward compatibility
			timeVal, ok = colValue.AsTimestamp()
			if !ok {
				return false, nil
			}
		}

		// Convert value to time.Time for comparison
		var compareTime time.Time
		switch val := value.(type) {
		case time.Time:
			compareTime = time.Date(1, 1, 1, val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), time.UTC)
		case string:
			var err error
			compareTime, err = storage.ParseTime(val)
			if err != nil {
				return false, fmt.Errorf("could not parse time string: %v", err)
			}
		default:
			return false, fmt.Errorf("cannot compare time with %T", value)
		}

		// For time comparison, ALWAYS normalize both to year 1 and use time.Time's comparison methods
		// This is critical for consistent behavior with TIME values that may have different dates
		norm1 := time.Date(1, 1, 1, timeVal.Hour(), timeVal.Minute(), timeVal.Second(), 0, time.UTC)
		norm2 := time.Date(1, 1, 1, compareTime.Hour(), compareTime.Minute(), compareTime.Second(), 0, time.UTC)

		// Apply the operator to the normalized times using time.Time's built-in methods
		switch operator {
		case storage.EQ:
			return norm1.Equal(norm2), nil
		case storage.NE:
			return !norm1.Equal(norm2), nil
		case storage.GT:
			return norm1.After(norm2), nil
		case storage.GTE:
			return norm1.After(norm2) || norm1.Equal(norm2), nil
		case storage.LT:
			return norm1.Before(norm2), nil
		case storage.LTE:
			return norm1.Before(norm2) || norm1.Equal(norm2), nil
		default:
			return false, fmt.Errorf("unsupported operator for time: %v", operator)
		}
	}

	// Default case - type not supported for comparison
	return false, fmt.Errorf("unsupported column type for comparison: %v", colValue.Type())
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
		Expr:        newExpr,
		Schema:      sae.Schema,
		ColumnNames: make([]string, len(sae.ColumnNames)),
		ColumnMap:   make(map[string]int, len(sae.ColumnMap)),
		aliases:     aliases,
	}

	// Copy the column names
	copy(result.ColumnNames, sae.ColumnNames)

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
		Expr:        sae.Expr,
		Schema:      sae.Schema,
		ColumnNames: make([]string, len(sae.ColumnNames)),
		ColumnMap:   make(map[string]int, len(columnMap)),
		aliases:     sae.aliases,
	}

	// Copy the column names
	copy(result.ColumnNames, sae.ColumnNames)

	// Use the provided column mapping
	for k, v := range columnMap {
		result.ColumnMap[k] = v
	}

	return result
}
