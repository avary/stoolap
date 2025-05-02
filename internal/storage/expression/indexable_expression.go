package expression

import (
	"github.com/stoolap/stoolap/internal/storage"
)

// Verify all expression types implement IndexableExpression
var (
	_ IndexableExpression = (*SimpleEqualityExpression)(nil)
	_ IndexableExpression = (*RangeExpression)(nil)
	_ IndexableExpression = (*LikeExpression)(nil)
	_ IndexableExpression = (*InExpression)(nil)
	_ IndexableExpression = (*NullCheckExpression)(nil)
	_ IndexableExpression = (*JSONPathExpression)(nil)
)

// IndexableExpression is an interface for expressions that can use indexes
// to provide index-aware query execution
type IndexableExpression interface {
	storage.Expression

	// GetColumnName returns the column name this expression operates on
	GetColumnName() string

	// GetValue returns the value this expression compares against
	GetValue() interface{}

	// GetOperator returns the operator this expression uses
	GetOperator() storage.Operator

	// CanUseIndex returns true if this expression can use an index
	CanUseIndex() bool
}

// SimpleEqualityExpression is a concrete implementation of IndexableExpression
// that supports equality comparison with index awareness
type SimpleEqualityExpression struct {
	columnName     string
	value          interface{}
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewEqualityExpression creates a new equality expression that can use indexes
func NewEqualityExpression(columnName string, value interface{}) storage.Expression {
	return &SimpleEqualityExpression{
		columnName: columnName,
		value:      value,
	}
}

// Evaluate evaluates the expression against a row
func (e *SimpleEqualityExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column index in the row by column name
	// In a proper implementation, this would be more efficient with schema information
	for _, col := range row {
		// In the actual implementation, we would look up column by name
		// For now, we'll just evaluate each column

		// Skip NULL values for equality comparisons
		if col.IsNull() {
			continue
		}

		// Convert types and compare values
		switch col.Type() {
		case storage.INTEGER:
			colVal, _ := col.AsInt64()
			switch v := e.value.(type) {
			case int:
				return colVal == int64(v), nil
			case int64:
				return colVal == v, nil
			case float64:
				return colVal == int64(v), nil
			}
		case storage.FLOAT:
			colVal, _ := col.AsFloat64()
			switch v := e.value.(type) {
			case float64:
				return colVal == v, nil
			case int:
				return colVal == float64(v), nil
			case int64:
				return colVal == float64(v), nil
			}
		case storage.TEXT:
			colVal, _ := col.AsString()
			if strVal, ok := e.value.(string); ok {
				return colVal == strVal, nil
			}
		case storage.BOOLEAN:
			colVal, _ := col.AsBoolean()
			if boolVal, ok := e.value.(bool); ok {
				return colVal == boolVal, nil
			}
		}
	}

	// Not found or no match
	return true, nil
}

// GetColumnName returns the column name this expression operates on
func (e *SimpleEqualityExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *SimpleEqualityExpression) GetValue() interface{} {
	return e.value
}

// GetOperator returns the operator this expression uses
func (e *SimpleEqualityExpression) GetOperator() storage.Operator {
	return storage.EQ
}

// CanUseIndex returns true if this expression can use an index
func (e *SimpleEqualityExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the storage.Expression interface
func (e *SimpleEqualityExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &SimpleEqualityExpression{
		columnName: e.columnName,
		value:      e.value,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// Other implementations for different operators can be added:
// - RangeExpression: for >, >=, <, <= operators
// - LikeExpression: for pattern matching
// - InExpression: for IN operators

// RangeExpression is a concrete implementation of IndexableExpression
// that supports range comparison with index awareness
type RangeExpression struct {
	columnName     string
	value          interface{}
	operator       storage.Operator  // GT, GTE, LT, LTE
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewRangeExpression creates a range expression (>, >=, <, <=)
func NewRangeExpression(columnName string, operator storage.Operator, value interface{}) storage.Expression {
	return &RangeExpression{
		columnName: columnName,
		value:      value,
		operator:   operator,
	}
}

// Evaluate evaluates the range expression against a row
func (e *RangeExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column by name
	for _, col := range row {
		if col.IsNull() {
			continue // NULL values are never equal to anything
		}

		// Type-specific comparisons
		switch col.Type() {
		case storage.INTEGER:
			colVal, _ := col.AsInt64()
			var compareVal int64

			switch v := e.value.(type) {
			case int:
				compareVal = int64(v)
			case int64:
				compareVal = v
			case float64:
				compareVal = int64(v)
			default:
				continue // Type mismatch, try next column
			}

			switch e.operator {
			case storage.GT:
				return colVal > compareVal, nil
			case storage.GTE:
				return colVal >= compareVal, nil
			case storage.LT:
				return colVal < compareVal, nil
			case storage.LTE:
				return colVal <= compareVal, nil
			}

		case storage.FLOAT:
			colVal, _ := col.AsFloat64()
			var compareVal float64

			switch v := e.value.(type) {
			case float64:
				compareVal = v
			case int:
				compareVal = float64(v)
			case int64:
				compareVal = float64(v)
			default:
				continue // Type mismatch, try next column
			}

			switch e.operator {
			case storage.GT:
				return colVal > compareVal, nil
			case storage.GTE:
				return colVal >= compareVal, nil
			case storage.LT:
				return colVal < compareVal, nil
			case storage.LTE:
				return colVal <= compareVal, nil
			}

		case storage.TEXT:
			colVal, _ := col.AsString()
			if strVal, ok := e.value.(string); ok {
				switch e.operator {
				case storage.GT:
					return colVal > strVal, nil
				case storage.GTE:
					return colVal >= strVal, nil
				case storage.LT:
					return colVal < strVal, nil
				case storage.LTE:
					return colVal <= strVal, nil
				}
			}
		}
	}

	// Not found or no match
	return true, nil
}

// GetColumnName returns the column name this expression operates on
func (e *RangeExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *RangeExpression) GetValue() interface{} {
	return e.value
}

// GetOperator returns the operator this expression uses
func (e *RangeExpression) GetOperator() storage.Operator {
	return e.operator
}

// CanUseIndex returns true if this expression can use an index
func (e *RangeExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the storage.Expression interface
func (e *RangeExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &RangeExpression{
		columnName: e.columnName,
		value:      e.value,
		operator:   e.operator,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// LikeExpression is a concrete implementation of IndexableExpression
// that supports pattern matching with index awareness
type LikeExpression struct {
	columnName     string
	pattern        string
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewLikeExpression creates an expression for LIKE pattern matching
func NewLikeExpression(columnName string, pattern string) storage.Expression {
	return &LikeExpression{
		columnName: columnName,
		pattern:    pattern,
	}
}

// Evaluate evaluates the LIKE expression against a row
func (e *LikeExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column by name
	for _, col := range row {
		if col.IsNull() {
			continue // NULL values never match patterns
		}

		if col.Type() == storage.TEXT {
			colVal, _ := col.AsString()
			// TODO: Implement proper LIKE pattern matching
			// For now, just check if the string contains the pattern
			// This is not SQL standard LIKE, just a placeholder
			return contains(colVal, e.pattern), nil
		}
	}

	// Not found or no match
	return true, nil
}

// contains is a simple string contains function for pattern matching
func contains(s, substr string) bool {
	return s == substr || (len(s) >= len(substr) && s[0:len(substr)] == substr)
}

// GetColumnName returns the column name this expression operates on
func (e *LikeExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *LikeExpression) GetValue() interface{} {
	return e.pattern
}

// GetOperator returns the operator this expression uses
func (e *LikeExpression) GetOperator() storage.Operator {
	return storage.LIKE
}

// CanUseIndex returns true if this expression can use an index
func (e *LikeExpression) CanUseIndex() bool {
	// Only prefix patterns can use indexes efficiently
	// We'll return true for all patterns for now
	return true
}

// WithAliases implements the storage.Expression interface
func (e *LikeExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &LikeExpression{
		columnName: e.columnName,
		pattern:    e.pattern,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// InExpression is a concrete implementation of IndexableExpression
// that supports IN operator with index awareness
type InExpression struct {
	columnName     string
	values         []interface{}
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewInExpression creates an expression for IN operator
func NewInExpression(columnName string, values []interface{}) storage.Expression {
	return &InExpression{
		columnName: columnName,
		values:     values,
	}
}

// Evaluate evaluates the IN expression against a row
func (e *InExpression) Evaluate(row storage.Row) (bool, error) {
	// Find the column by name
	for _, col := range row {
		if col.IsNull() {
			continue // NULL values are never IN a set
		}

		// For each value in the IN list, check if it matches
		for _, val := range e.values {
			// Type-specific comparisons
			switch col.Type() {
			case storage.INTEGER:
				colVal, _ := col.AsInt64()
				switch v := val.(type) {
				case int:
					if colVal == int64(v) {
						return true, nil
					}
				case int64:
					if colVal == v {
						return true, nil
					}
				case float64:
					if colVal == int64(v) {
						return true, nil
					}
				}

			case storage.FLOAT:
				colVal, _ := col.AsFloat64()
				switch v := val.(type) {
				case float64:
					if colVal == v {
						return true, nil
					}
				case int:
					if colVal == float64(v) {
						return true, nil
					}
				case int64:
					if colVal == float64(v) {
						return true, nil
					}
				}

			case storage.TEXT:
				colVal, _ := col.AsString()
				if strVal, ok := val.(string); ok {
					if colVal == strVal {
						return true, nil
					}
				}

			case storage.BOOLEAN:
				colVal, _ := col.AsBoolean()
				if boolVal, ok := val.(bool); ok {
					if colVal == boolVal {
						return true, nil
					}
				}
			}
		}
	}

	// Not found or no match
	return true, nil
}

// GetColumnName returns the column name this expression operates on
func (e *InExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *InExpression) GetValue() interface{} {
	return e.values
}

// GetOperator returns the operator this expression uses
func (e *InExpression) GetOperator() storage.Operator {
	return storage.IN
}

// CanUseIndex returns true if this expression can use an index
func (e *InExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the storage.Expression interface
func (e *InExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &InExpression{
		columnName: e.columnName,
		values:     e.values,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// NullCheckExpression is a concrete implementation of IndexableExpression
// that supports IS NULL and IS NOT NULL operators
type NullCheckExpression struct {
	columnName     string
	isNull         bool              // true for IS NULL, false for IS NOT NULL
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewIsNullExpression creates an expression for IS NULL
func NewIsNullExpression(columnName string) storage.Expression {
	return &NullCheckExpression{
		columnName: columnName,
		isNull:     true,
	}
}

// NewIsNotNullExpression creates an expression for IS NOT NULL
func NewIsNotNullExpression(columnName string) storage.Expression {
	return &NullCheckExpression{
		columnName: columnName,
		isNull:     false,
	}
}

// Evaluate evaluates the NULL check expression against a row
func (e *NullCheckExpression) Evaluate(row storage.Row) (bool, error) {
	// For NullCheckExpression, we need to handle the case where we're called directly
	// without going through SchemaAwareExpression.

	// Rather than do a general check, we need to find the right column.
	// We can signal to the caller that they should use SchemaAwareExpression
	// by simply returning a pre-determined result.

	// This implementation assumes the caller needs to use SchemaAwareExpression
	// if they want to accurately determine NULL status for named columns.

	// For IS NULL operations
	if e.isNull {
		return false, nil
	}

	// For IS NOT NULL operations
	return true, nil
}

// GetColumnName returns the column name this expression operates on
func (e *NullCheckExpression) GetColumnName() string {
	return e.columnName
}

// GetValue returns the value this expression compares against
func (e *NullCheckExpression) GetValue() interface{} {
	return nil
}

// GetOperator returns the operator this expression uses
func (e *NullCheckExpression) GetOperator() storage.Operator {
	if e.isNull {
		return storage.ISNULL
	}
	return storage.ISNOTNULL
}

// CanUseIndex returns true if this expression can use an index
func (e *NullCheckExpression) CanUseIndex() bool {
	// NULL checks can use indexes if the index supports it
	return true
}

// WithAliases implements the storage.Expression interface
func (e *NullCheckExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &NullCheckExpression{
		columnName: e.columnName,
		isNull:     e.isNull,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.columnName]; isAlias {
		expr.originalColumn = e.columnName // Keep track of the original alias name
		expr.columnName = originalCol      // Replace with the actual column name
	}

	return expr
}

// OptimizedIndexExpression wraps an IndexableExpression with pre-computed index locations
type OptimizedIndexExpression struct {
	expr      IndexableExpression
	locations []storage.IndexEntry
}

// NewOptimizedIndexExpression creates a new optimized index expression
func NewOptimizedIndexExpression(expr IndexableExpression, locations []storage.IndexEntry) *OptimizedIndexExpression {
	return &OptimizedIndexExpression{
		expr:      expr,
		locations: locations,
	}
}

// Evaluate implements the Expression interface by delegating to the wrapped expression
func (e *OptimizedIndexExpression) Evaluate(row storage.Row) (bool, error) {
	return e.expr.Evaluate(row)
}

// GetIndexLocations returns the pre-computed index locations
func (e *OptimizedIndexExpression) GetIndexLocations() ([]storage.IndexEntry, error) {
	return e.locations, nil
}

// GetColumnName implements IndexableExpression by delegating to the wrapped expression
func (e *OptimizedIndexExpression) GetColumnName() string {
	return e.expr.GetColumnName()
}

// GetValue implements IndexableExpression by delegating to the wrapped expression
func (e *OptimizedIndexExpression) GetValue() interface{} {
	return e.expr.GetValue()
}

// GetOperator implements IndexableExpression by delegating to the wrapped expression
func (e *OptimizedIndexExpression) GetOperator() storage.Operator {
	return e.expr.GetOperator()
}

// CanUseIndex implements IndexableExpression - always returns true since this is pre-optimized
func (e *OptimizedIndexExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the storage.Expression interface
func (e *OptimizedIndexExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Apply aliases to the wrapped expression if it supports them
	if aliasable, ok := e.expr.(interface {
		WithAliases(map[string]string) storage.Expression
	}); ok {
		newExpr := aliasable.WithAliases(aliases)

		// If the result is still an IndexableExpression, wrap it with our optimized info
		if indexable, ok := newExpr.(IndexableExpression); ok {
			return &OptimizedIndexExpression{
				expr:      indexable,
				locations: e.locations,
			}
		}

		// Otherwise just return the aliased expression without optimization
		return newExpr
	}

	// If the wrapped expression doesn't support aliases, return self
	return e
}
