package expression

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// CastExpression represents a CAST(column AS type) expression
type CastExpression struct {
	Column     string            // The column name to cast
	TargetType storage.DataType  // The target data type to cast to
	aliases    map[string]string // Column aliases (alias -> original name)
}

// NewCastExpression creates a new cast expression
func NewCastExpression(column string, targetType storage.DataType) *CastExpression {
	return &CastExpression{
		Column:     column,
		TargetType: targetType,
		aliases:    make(map[string]string),
	}
}

// Evaluate implements the Expression interface for CastExpression
func (ce *CastExpression) Evaluate(row storage.Row) (bool, error) {
	// Since CastExpression is typically used in a comparison,
	// just returning true means it will be evaluated by its parent expression
	// This is not ideal for standalone usage, but works in WHERE clauses
	// where the cast is part of a comparison
	return true, nil
}

// WithAliases implements the storage.Expression interface
func (ce *CastExpression) WithAliases(aliases map[string]string) storage.Expression {
	result := &CastExpression{
		Column:     ce.Column,
		TargetType: ce.TargetType,
		aliases:    make(map[string]string),
	}

	// Copy aliases
	for k, v := range aliases {
		result.aliases[k] = v
	}

	// Apply aliases to column name
	if origCol, ok := aliases[ce.Column]; ok {
		result.Column = origCol
	}

	return result
}

// GetColumnName returns the column name this expression operates on
func (ce *CastExpression) GetColumnName() string {
	return ce.Column
}

// GetTargetType returns the target data type for the cast
func (ce *CastExpression) GetTargetType() storage.DataType {
	return ce.TargetType
}

// PerformCast performs the actual cast operation on a column value
func (ce *CastExpression) PerformCast(value storage.ColumnValue) (storage.ColumnValue, error) {
	if value == nil || value.IsNull() {
		return storage.NewNullValue(ce.TargetType), nil
	}

	switch ce.TargetType {
	case storage.INTEGER:
		return castToInteger(value)
	case storage.FLOAT:
		return castToFloat(value)
	case storage.TEXT:
		return castToString(value)
	case storage.BOOLEAN:
		return castToBoolean(value)
	case storage.DATE:
		return castToDate(value)
	case storage.TIMESTAMP:
		return castToTimestamp(value)
	case storage.TIME:
		return castToTime(value)
	case storage.JSON:
		return castToJSON(value)
	default:
		return nil, fmt.Errorf("unsupported cast target type: %v", ce.TargetType)
	}
}

// CompoundExpression represents a CAST(column AS type) operator value expression
// This is used to handle WHERE clauses with CAST like: WHERE CAST(column AS INTEGER) > 100
type CompoundExpression struct {
	CastExpr *CastExpression   // The CAST expression
	Operator storage.Operator  // The comparison operator
	Value    interface{}       // The value to compare against
	aliases  map[string]string // Column aliases (alias -> original)
}

// Evaluate implements the Expression interface for CompoundExpression
func (ce *CompoundExpression) Evaluate(row storage.Row) (bool, error) {
	// Find column index by name
	colIndex := -1
	colName := ce.CastExpr.Column

	// Try to find the column in the row
	for i, val := range row {
		// Skip nil values
		if val == nil {
			continue
		}

		// Check if column value has a name method
		if colVal, ok := val.(interface{ Name() string }); ok {
			name := colVal.Name()
			if name == colName || strings.EqualFold(name, colName) {
				colIndex = i
				break
			}
		}
	}

	// Column not found
	if colIndex == -1 || colIndex >= len(row) {
		return false, fmt.Errorf("column '%s' not found in row", colName)
	}

	// Get the column value
	colValue := row[colIndex]

	if colValue == nil || colValue.IsNull() {
		// NULL values always return false for comparisons
		return false, nil
	}

	// Cast the column value to the target type
	castedValue, err := ce.CastExpr.PerformCast(colValue)
	if err != nil {
		return false, err
	}

	// Now compare the casted value with the comparison value
	// Convert literal value to column value for comparison
	var comparisonValue storage.ColumnValue

	switch ce.CastExpr.TargetType {
	case storage.INTEGER:
		// Convert comparison value to integer
		var intVal int64
		switch v := ce.Value.(type) {
		case int:
			intVal = int64(v)
		case int64:
			intVal = v
		case float64:
			intVal = int64(v)
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				intVal = parsed
			} else {
				return false, fmt.Errorf("cannot convert comparison value %v to INTEGER", ce.Value)
			}
		default:
			return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
		}
		comparisonValue = storage.NewIntegerValue(intVal)

	case storage.FLOAT:
		// Convert comparison value to float
		var floatVal float64
		switch v := ce.Value.(type) {
		case int:
			floatVal = float64(v)
		case int64:
			floatVal = float64(v)
		case float64:
			floatVal = v
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				floatVal = parsed
			} else {
				return false, fmt.Errorf("cannot convert comparison value %v to FLOAT", ce.Value)
			}
		default:
			return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
		}
		comparisonValue = storage.NewFloatValue(floatVal)

	case storage.TEXT:
		// Convert comparison value to string
		var strVal string
		switch v := ce.Value.(type) {
		case string:
			strVal = v
		default:
			strVal = fmt.Sprintf("%v", ce.Value)
		}
		comparisonValue = storage.NewStringValue(strVal)

	case storage.BOOLEAN:
		// Convert comparison value to boolean
		var boolVal bool
		switch v := ce.Value.(type) {
		case bool:
			boolVal = v
		case int:
			boolVal = v != 0
		case int64:
			boolVal = v != 0
		case float64:
			boolVal = v != 0
		case string:
			s := strings.ToLower(v)
			boolVal = s == "true" || s == "1" || s == "t" || s == "yes" || s == "y"
		default:
			return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
		}
		comparisonValue = storage.NewBooleanValue(boolVal)

	default:
		// For other types, just use string conversion as fallback
		comparisonValue = storage.NewStringValue(fmt.Sprintf("%v", ce.Value))
	}

	// Compare the values based on the operator
	var result bool
	switch ce.Operator {
	case storage.EQ:
		result = compareColumnValues(castedValue, comparisonValue) == 0
	case storage.NE:
		result = compareColumnValues(castedValue, comparisonValue) != 0
	case storage.GT:
		result = compareColumnValues(castedValue, comparisonValue) > 0
	case storage.GTE:
		result = compareColumnValues(castedValue, comparisonValue) >= 0
	case storage.LT:
		result = compareColumnValues(castedValue, comparisonValue) < 0
	case storage.LTE:
		result = compareColumnValues(castedValue, comparisonValue) <= 0
	default:
		return false, fmt.Errorf("unsupported operator: %v", ce.Operator)
	}

	return result, nil
}

// WithAliases implements the storage.Expression interface
func (ce *CompoundExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same properties
	result := &CompoundExpression{
		CastExpr: ce.CastExpr,
		Operator: ce.Operator,
		Value:    ce.Value,
		aliases:  make(map[string]string),
	}

	// Apply aliases to the cast expression
	if castWithAliases, ok := ce.CastExpr.WithAliases(aliases).(*CastExpression); ok {
		result.CastExpr = castWithAliases
	}

	// Copy aliases
	for k, v := range aliases {
		result.aliases[k] = v
	}

	return result
}

// compareColumnValues compares two column values
// Returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
func compareColumnValues(a, b storage.ColumnValue) int {
	// Handle NULL values
	if a == nil || a.IsNull() || b == nil || b.IsNull() {
		// NULL is considered equal only to NULL
		if (a == nil || a.IsNull()) && (b == nil || b.IsNull()) {
			return 0
		}
		// NULL is less than everything else
		if a == nil || a.IsNull() {
			return -1
		}
		return 1
	}

	// Compare based on types
	aType := a.Type()
	bType := b.Type()

	// If types are the same, compare directly
	if aType == bType {
		switch aType {
		case storage.INTEGER:
			aVal, _ := a.AsInt64()
			bVal, _ := b.AsInt64()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.FLOAT:
			aVal, _ := a.AsFloat64()
			bVal, _ := b.AsFloat64()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.TEXT:
			aVal, _ := a.AsString()
			bVal, _ := b.AsString()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.BOOLEAN:
			aVal, _ := a.AsBoolean()
			bVal, _ := b.AsBoolean()
			if !aVal && bVal {
				return -1
			} else if aVal && !bVal {
				return 1
			}
			return 0

		case storage.TIMESTAMP, storage.DATE, storage.TIME:
			aVal, _ := a.AsTimestamp()
			bVal, _ := b.AsTimestamp()
			if aVal.Before(bVal) {
				return -1
			} else if aVal.After(bVal) {
				return 1
			}
			return 0

		default:
			// Fallback to string comparison
			aVal, _ := a.AsString()
			bVal, _ := b.AsString()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	}

	// Types differ, try numeric comparison for numeric types
	if (aType == storage.INTEGER || aType == storage.FLOAT) &&
		(bType == storage.INTEGER || bType == storage.FLOAT) {
		// Convert both to float for comparison
		aVal, _ := a.AsFloat64()
		bVal, _ := b.AsFloat64()
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}

	// Fallback to string comparison
	aStr, _ := a.AsString()
	bStr, _ := b.AsString()
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// Cast helpers

func castToInteger(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Already an integer, return as is
		i, _ := value.AsInt64()
		return storage.NewIntegerValue(i), nil
	case storage.FLOAT:
		// Convert float to integer
		f, _ := value.AsFloat64()
		return storage.NewIntegerValue(int64(f)), nil
	case storage.TEXT:
		// Parse string to integer
		s, _ := value.AsString()
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// If string can't be parsed as int, try to parse as float first
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return storage.NewIntegerValue(int64(f)), nil
			}
			// Return 0 for invalid string to integer conversions
			return storage.NewIntegerValue(0), nil
		}
		return storage.NewIntegerValue(i), nil
	case storage.BOOLEAN:
		// Convert boolean to integer (true=1, false=0)
		b, _ := value.AsBoolean()
		if b {
			return storage.NewIntegerValue(1), nil
		}
		return storage.NewIntegerValue(0), nil
	case storage.TIMESTAMP, storage.DATE, storage.TIME:
		// Convert timestamp to Unix timestamp (seconds since epoch)
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewIntegerValue(0), nil
		}
		return storage.NewIntegerValue(t.Unix()), nil
	default:
		// Default to 0 for unsupported types
		return storage.NewIntegerValue(0), nil
	}
}

func castToFloat(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Convert integer to float
		i, _ := value.AsInt64()
		return storage.NewFloatValue(float64(i)), nil
	case storage.FLOAT:
		// Already a float, return as is
		f, _ := value.AsFloat64()
		return storage.NewFloatValue(f), nil
	case storage.TEXT:
		// Parse string to float
		s, _ := value.AsString()
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			// Return 0.0 for invalid string to float conversions
			return storage.NewFloatValue(0.0), nil
		}
		return storage.NewFloatValue(f), nil
	case storage.BOOLEAN:
		// Convert boolean to float (true=1.0, false=0.0)
		b, _ := value.AsBoolean()
		if b {
			return storage.NewFloatValue(1.0), nil
		}
		return storage.NewFloatValue(0.0), nil
	case storage.TIMESTAMP, storage.DATE, storage.TIME:
		// Convert timestamp to Unix timestamp as float
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewFloatValue(0.0), nil
		}
		return storage.NewFloatValue(float64(t.Unix())), nil
	default:
		// Default to 0.0 for unsupported types
		return storage.NewFloatValue(0.0), nil
	}
}

func castToString(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Convert integer to string
		i, _ := value.AsInt64()
		return storage.NewStringValue(strconv.FormatInt(i, 10)), nil
	case storage.FLOAT:
		// Convert float to string
		f, _ := value.AsFloat64()
		return storage.NewStringValue(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case storage.TEXT:
		// Already a string, return as is
		s, _ := value.AsString()
		return storage.NewStringValue(s), nil
	case storage.BOOLEAN:
		// Convert boolean to string
		b, _ := value.AsBoolean()
		if b {
			return storage.NewStringValue("true"), nil
		}
		return storage.NewStringValue("false"), nil
	case storage.TIMESTAMP:
		// Format timestamp as string (ISO format)
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewStringValue(""), nil
		}
		return storage.NewStringValue(t.Format(time.RFC3339)), nil
	case storage.DATE:
		// Format date as string (YYYY-MM-DD)
		d, ok := value.AsDate()
		if !ok {
			return storage.NewStringValue(""), nil
		}
		return storage.NewStringValue(d.Format("2006-01-02")), nil
	case storage.TIME:
		// Format time as string (HH:MM:SS)
		t, ok := value.AsTime()
		if !ok {
			return storage.NewStringValue(""), nil
		}
		return storage.NewStringValue(t.Format("15:04:05")), nil
	case storage.JSON:
		// JSON as string (the raw JSON string)
		s, _ := value.AsString()
		return storage.NewStringValue(s), nil
	default:
		// Default to empty string for unsupported types
		return storage.NewStringValue(""), nil
	}
}

func castToBoolean(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// 0 is false, anything else is true
		i, _ := value.AsInt64()
		return storage.NewBooleanValue(i != 0), nil
	case storage.FLOAT:
		// 0.0 is false, anything else is true
		f, _ := value.AsFloat64()
		return storage.NewBooleanValue(f != 0.0), nil
	case storage.TEXT:
		// Parse string to boolean
		s, _ := value.AsString()
		s = strings.ToLower(s)
		return storage.NewBooleanValue(s == "true" || s == "1" || s == "t" || s == "yes" || s == "y"), nil
	case storage.BOOLEAN:
		// Already a boolean, return as is
		b, _ := value.AsBoolean()
		return storage.NewBooleanValue(b), nil
	default:
		// Default to false for unsupported types
		return storage.NewBooleanValue(false), nil
	}
}

func castToDate(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.TIMESTAMP:
		// Extract date part from timestamp
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewDateValue(time.Now()), nil
		}
		// Remove time component
		date := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		return storage.NewDateValue(date), nil
	case storage.DATE:
		// Already a date, return as is
		d, _ := value.AsDate()
		return storage.NewDateValue(d), nil
	case storage.TEXT:
		// Parse string to date
		s, _ := value.AsString()
		// Try various date formats
		if d, err := storage.ParseDate(s); err == nil {
			return storage.NewDateValue(d), nil
		}
		// Default to current date if parsing fails
		return storage.NewDateValue(time.Now()), nil
	default:
		// Default to current date for unsupported types
		return storage.NewDateValue(time.Now()), nil
	}
}

func castToTimestamp(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Interpret integer as Unix timestamp
		i, _ := value.AsInt64()
		return storage.NewTimestampValue(time.Unix(i, 0)), nil
	case storage.FLOAT:
		// Interpret float as Unix timestamp
		f, _ := value.AsFloat64()
		return storage.NewTimestampValue(time.Unix(int64(f), 0)), nil
	case storage.TIMESTAMP:
		// Already a timestamp, return as is
		t, _ := value.AsTimestamp()
		return storage.NewTimestampValue(t), nil
	case storage.DATE:
		// Convert date to timestamp (midnight on that date)
		d, _ := value.AsDate()
		return storage.NewTimestampValue(d), nil
	case storage.TEXT:
		// Parse string to timestamp
		s, _ := value.AsString()
		if t, err := storage.ParseTimestamp(s); err == nil {
			return storage.NewTimestampValue(t), nil
		}
		// Default to current timestamp if parsing fails
		return storage.NewTimestampValue(time.Now()), nil
	default:
		// Default to current timestamp for unsupported types
		return storage.NewTimestampValue(time.Now()), nil
	}
}

func castToTime(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.TIME:
		// Already a time, return as is
		t, _ := value.AsTime()
		return storage.NewTimeValue(t), nil
	case storage.TIMESTAMP:
		// Extract time part from timestamp
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewTimeValue(time.Now()), nil
		}
		timeOnly := time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
		return storage.NewTimeValue(timeOnly), nil
	case storage.TEXT:
		// Parse string to time
		s, _ := value.AsString()
		if t, err := storage.ParseTime(s); err == nil {
			timeOnly := time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
			return storage.NewTimeValue(timeOnly), nil
		}
		// Default to current time if parsing fails
		now := time.Now()
		timeOnly := time.Date(1, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), now.Location())
		return storage.NewTimeValue(timeOnly), nil
	default:
		// Default to current time for unsupported types
		now := time.Now()
		timeOnly := time.Date(1, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), now.Location())
		return storage.NewTimeValue(timeOnly), nil
	}
}

func castToJSON(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.JSON:
		// Already JSON, return as is
		j, _ := value.AsString()
		return storage.NewJSONValue(j), nil
	case storage.TEXT:
		// String to JSON (assuming the string is valid JSON)
		s, _ := value.AsString()
		return storage.NewJSONValue(s), nil
	default:
		// For other types, convert to string and wrap as JSON string
		s := ""
		switch value.Type() {
		case storage.INTEGER:
			i, _ := value.AsInt64()
			s = strconv.FormatInt(i, 10)
		case storage.FLOAT:
			f, _ := value.AsFloat64()
			s = strconv.FormatFloat(f, 'f', -1, 64)
		case storage.BOOLEAN:
			b, _ := value.AsBoolean()
			if b {
				s = "true"
			} else {
				s = "false"
			}
		default:
			s = "null"
		}
		return storage.NewJSONValue(s), nil
	}
}
