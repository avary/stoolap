package storage

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	// Static NULL value instances for better performance and reduced allocations
	// These shared instances are safe to use since null values are immutable
	StaticNullUnknown   = &DirectValue{dataType: NULL, isNull: true}
	StaticNullInteger   = &DirectValue{dataType: INTEGER, isNull: true}
	StaticNullFloat     = &DirectValue{dataType: FLOAT, isNull: true}
	StaticNullString    = &DirectValue{dataType: TEXT, isNull: true}
	StaticNullBoolean   = &DirectValue{dataType: BOOLEAN, isNull: true}
	StaticNullTimestamp = &DirectValue{dataType: TIMESTAMP, isNull: true}
	StaticNullJSON      = &DirectValue{dataType: JSON, isNull: true}

	// Map for fast lookup by data type
	staticNullValuesByType = map[DataType]ColumnValue{
		NULL:      StaticNullUnknown,
		INTEGER:   StaticNullInteger,
		FLOAT:     StaticNullFloat,
		TEXT:      StaticNullString,
		BOOLEAN:   StaticNullBoolean,
		TIMESTAMP: StaticNullTimestamp,
		JSON:      StaticNullJSON,
	}

	// Static common integer values
	staticZeroInt = &DirectValue{dataType: INTEGER, intValue: 0, valueRef: int64(0)}
	staticOneInt  = &DirectValue{dataType: INTEGER, intValue: 1, valueRef: int64(1)}

	// Static common float values
	staticZeroFloat = &DirectValue{dataType: FLOAT, floatValue: 0.0, valueRef: 0.0}
	staticOneFloat  = &DirectValue{dataType: FLOAT, floatValue: 1.0, valueRef: 1.0}

	// Static common string values
	staticEmptyString = &DirectValue{dataType: TEXT, stringValue: "", valueRef: ""}

	// Static boolean values for true and false
	staticBooleanTrue  = &DirectValue{dataType: BOOLEAN, boolValue: true, valueRef: true}
	staticBooleanFalse = &DirectValue{dataType: BOOLEAN, boolValue: false, valueRef: false}
)

// DirectValue is a wrapper that stores primitive values directly
// and keeps an interface{} reference for efficient scanning
type DirectValue struct {
	dataType    DataType
	isNull      bool
	intValue    int64
	floatValue  float64
	stringValue string
	boolValue   bool
	timeValue   time.Time
	// valueRef holds a reference to the original value for efficient interface{} assignment
	valueRef interface{}

	pooled atomic.Bool // Indicates if this value is pooled
}

// Type returns the column value type
func (v *DirectValue) Type() DataType {
	return v.dataType
}

// IsNull returns true if the value is NULL
func (v *DirectValue) IsNull() bool {
	return v.isNull
}

// AsInt64 returns the integer value
func (v *DirectValue) AsInt64() (int64, bool) {
	if v.isNull {
		return 0, true
	}

	switch v.dataType {
	case INTEGER:
		return v.intValue, true
	case FLOAT:
		// Try to convert float to integer
		return int64(v.floatValue), true
	case TEXT:
		// Try to convert string to integer
		if i, err := strconv.ParseInt(v.stringValue, 10, 64); err == nil {
			return i, true
		}
		// Try float parsing and conversion
		if f, err := strconv.ParseFloat(v.stringValue, 64); err == nil {
			return int64(f), true
		}
		return 0, false
	case BOOLEAN:
		// true = 1, false = 0
		if v.boolValue {
			return 1, true
		}
		return 0, true
	case TIMESTAMP:
		return v.timeValue.UTC().UnixNano(), true
	default:
		// Return 0 with ok=false for other types
		return 0, false
	}
}

// AsFloat64 returns the float value
func (v *DirectValue) AsFloat64() (float64, bool) {
	if v.isNull {
		return 0, true
	}

	switch v.dataType {
	case FLOAT:
		return v.floatValue, true
	case INTEGER:
		// Convert integer to float
		return float64(v.intValue), true
	case TEXT:
		// Try to convert string to float
		if f, err := strconv.ParseFloat(v.stringValue, 64); err == nil {
			return f, true
		}
		return 0, false
	case BOOLEAN:
		// true = 1.0, false = 0.0
		if v.boolValue {
			return 1.0, true
		}
		return 0.0, true
	default:
		// Return 0 with ok=false for other types
		return 0, false
	}
}

// AsBoolean returns the boolean value
func (v *DirectValue) AsBoolean() (bool, bool) {
	if v.isNull {
		return false, true
	}

	switch v.dataType {
	case BOOLEAN:
		return v.boolValue, true
	case INTEGER:
		// Non-zero integers are true
		return v.intValue != 0, true
	case FLOAT:
		// Non-zero floats are true
		return v.floatValue != 0, true
	case TEXT:
		// Try to parse boolean from string
		s := strings.ToLower(v.stringValue)
		if s == "true" || s == "t" || s == "yes" || s == "y" || s == "1" {
			return true, true
		} else if s == "false" || s == "f" || s == "no" || s == "n" || s == "0" || s == "" {
			return false, true
		}
		// Try to parse as a number
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f != 0, true
		}
		return false, false
	default:
		// Return false with ok=false for other types
		return false, false
	}
}

// AsString returns the string value
func (v *DirectValue) AsString() (string, bool) {
	if v.isNull {
		return "", true
	}

	switch v.dataType {
	case TEXT, JSON:
		return v.stringValue, true
	case INTEGER:
		return strconv.FormatInt(v.intValue, 10), true
	case FLOAT:
		return strconv.FormatFloat(v.floatValue, 'g', -1, 64), true
	case BOOLEAN:
		if v.boolValue {
			return "true", true
		}
		return "false", true
	case TIMESTAMP:
		return v.timeValue.Format(time.RFC3339), true
	default:
		return "", false
	}
}

// AsTimestamp returns the timestamp value
func (v *DirectValue) AsTimestamp() (time.Time, bool) {
	if v.isNull {
		return time.Time{}, true
	}

	if v.dataType == TIMESTAMP {
		return v.timeValue, true
	}
	return time.Time{}, false
}

// AsJSON returns the JSON value
func (v *DirectValue) AsJSON() (string, bool) {
	if v.isNull {
		return "{}", true
	}

	if v.dataType == JSON {
		return v.stringValue, true
	}
	return "{}", false
}

// AsInterface returns the original reference value directly, avoiding allocation
func (v *DirectValue) AsInterface() interface{} {
	if v.isNull {
		return nil
	}

	return v.valueRef
}

func (v *DirectValue) Equals(other ColumnValue) bool {
	// Handle nil case
	if other == nil {
		return false
	}

	// Handle null values
	if v.IsNull() && other.IsNull() {
		return true
	}
	if v.IsNull() || other.IsNull() {
		return false
	}

	// Type must match
	if v.Type() != other.Type() {
		return false
	}

	// Type-specific comparison
	switch v.Type() {
	case INTEGER:
		v1, _ := v.AsInt64()
		v2, _ := other.AsInt64()
		return v1 == v2

	case FLOAT:
		v1, _ := v.AsFloat64()
		v2, _ := other.AsFloat64()
		return v1 == v2

	case TEXT, JSON:
		v1, _ := v.AsString()
		v2, _ := other.AsString()
		return v1 == v2

	case BOOLEAN:
		v1, _ := v.AsBoolean()
		v2, _ := other.AsBoolean()
		return v1 == v2

	case TIMESTAMP:
		v1, _ := v.AsTimestamp()
		v2, _ := other.AsTimestamp()
		return v1.Equal(v2)

	default:
		// For unknown types, compare string representation
		v1, _ := v.AsString()
		v2, _ := other.AsString()
		return v1 == v2
	}
}

// Compare compares two values and returns:
// -1 if v < other
// 0 if v == other
// 1 if v > other
// error if the comparison is not possible
func (v *DirectValue) Compare(other ColumnValue) (int, error) {
	// NULL comparisons
	if v.IsNull() || other == nil || other.IsNull() {
		// NULL is only equal to NULL, otherwise comparison is undefined
		if v.IsNull() && (other == nil || other.IsNull()) {
			return 0, nil
		}
		return 0, ErrNullComparison
	}

	// Direct comparison for same types (most efficient path)
	if v.Type() == other.Type() {
		switch v.Type() {
		case INTEGER:
			v1, _ := v.AsInt64()
			v2, _ := other.AsInt64()
			if v1 < v2 {
				return -1, nil
			} else if v1 > v2 {
				return 1, nil
			}
			return 0, nil

		case FLOAT:
			v1, _ := v.AsFloat64()
			v2, _ := other.AsFloat64()
			if v1 < v2 {
				return -1, nil
			} else if v1 > v2 {
				return 1, nil
			}
			return 0, nil

		case TEXT:
			v1, _ := v.AsString()
			v2, _ := other.AsString()
			if v1 < v2 {
				return -1, nil
			} else if v1 > v2 {
				return 1, nil
			}
			return 0, nil

		case BOOLEAN:
			v1, _ := v.AsBoolean()
			v2, _ := other.AsBoolean()
			if !v1 && v2 {
				return -1, nil
			} else if v1 && !v2 {
				return 1, nil
			}
			return 0, nil

		case TIMESTAMP:
			v1, _ := v.AsTimestamp()
			v2, _ := other.AsTimestamp()
			return v1.Compare(v2), nil

		case JSON:
			// For JSON, we can only test equality, not ordering
			v1, _ := v.AsString()
			v2, _ := other.AsString()
			if v1 == v2 {
				return 0, nil
			}
			return 0, ErrIncomparableTypes
		}
	}

	// Cross-type numeric comparisons (integer vs float)
	if (v.Type() == INTEGER && other.Type() == FLOAT) || (v.Type() == FLOAT && other.Type() == INTEGER) {
		var v1, v2 float64

		if v.Type() == INTEGER {
			i, _ := v.AsInt64()
			v1 = float64(i)
		} else {
			v1, _ = v.AsFloat64()
		}

		if other.Type() == INTEGER {
			i, _ := other.AsInt64()
			v2 = float64(i)
		} else {
			v2, _ = other.AsFloat64()
		}

		if v1 < v2 {
			return -1, nil
		} else if v1 > v2 {
			return 1, nil
		}
		return 0, nil
	}

	// String-based comparisons for mixed types
	// This is less efficient but handles heterogeneous comparisons
	v1, _ := v.AsString()
	v2, _ := other.AsString()

	if v1 < v2 {
		return -1, nil
	} else if v1 > v2 {
		return 1, nil
	}
	return 0, nil
}

// NewDirectValue creates a new direct value with the appropriate type
func NewDirectValue(dataType DataType) *DirectValue {
	v := new(DirectValue)
	v.dataType = dataType

	return v
}

// NewDirectValueFromInterface creates a new DirectValue from an interface{} value
// inferring the type automatically
func NewDirectValueFromInterface(val interface{}) *DirectValue {
	if val == nil {
		return StaticNullUnknown
	}

	// Use type switch for common types to avoid reflection
	switch v := val.(type) {
	case int:
		dv := NewDirectValue(INTEGER)
		dv.intValue = int64(v)
		dv.valueRef = v
		return dv
	case int64:
		dv := NewDirectValue(INTEGER)
		dv.intValue = v
		dv.valueRef = v
		return dv
	case float32:
		dv := NewDirectValue(FLOAT)
		dv.floatValue = float64(v)
		dv.valueRef = float64(v)
		return dv
	case float64:
		dv := NewDirectValue(FLOAT)
		dv.floatValue = v
		dv.valueRef = v
		return dv
	case string:
		dv := NewDirectValue(TEXT)
		dv.stringValue = v
		dv.valueRef = v
		return dv
	case bool:
		// Use static boolean values for better performance
		if v {
			return staticBooleanTrue
		}
		return staticBooleanFalse
	case time.Time:
		dv := NewDirectValue(TIMESTAMP)
		dv.timeValue = v.UTC()
		dv.valueRef = v.UTC()
		return dv
	case []byte:
		// Try to parse as JSON
		var jsonObj interface{}
		if json.Unmarshal(v, &jsonObj) == nil {
			dv := NewDirectValue(JSON)
			dv.stringValue = string(v)
			dv.valueRef = jsonObj
			return dv
		}
		// Fall back to string
		dv := NewDirectValue(TEXT)
		dv.stringValue = string(v)
		dv.valueRef = dv.stringValue
		return dv
	case map[string]interface{}, []interface{}:
		// JSON object
		jsonBytes, _ := json.Marshal(v)
		dv := NewDirectValue(JSON)
		dv.stringValue = string(jsonBytes)
		dv.valueRef = v
		return dv
	default:
		// Fall back to string representation
		str := fmt.Sprintf("%v", v)
		dv := NewDirectValue(TEXT)
		dv.stringValue = str
		dv.valueRef = str
		return dv
	}
}

// NewNullDirectValue creates a NULL value of the specified type
func NewNullDirectValue(dataType DataType) *DirectValue {
	// Use static null values when available for better performance
	if staticVal, ok := staticNullValuesByType[dataType]; ok {
		return staticVal.(*DirectValue)
	}

	// Fallback for any data types not in the static map
	v := new(DirectValue)
	v.dataType = dataType
	v.isNull = true

	return v
}

// Type-specific value creation helpers

// NewIntegerValue creates a new integer value
func NewIntegerValue(value int64) ColumnValue {
	v := NewDirectValue(INTEGER)
	v.intValue = value
	v.valueRef = value // Store reference to original value
	return v
}

// NewFloatValue creates a new float value
func NewFloatValue(value float64) ColumnValue {
	v := NewDirectValue(FLOAT)
	v.floatValue = value
	v.valueRef = value // Store reference to original value
	return v
}

// NewStringValue creates a new string value
func NewStringValue(value string) ColumnValue {
	v := NewDirectValue(TEXT)
	v.stringValue = value
	v.valueRef = value // Store reference to original value
	return v
}

// NewBooleanValue creates a new boolean value
func NewBooleanValue(value bool) ColumnValue {
	// Use static boolean values for better performance
	if value {
		return staticBooleanTrue
	}
	return staticBooleanFalse
}

// NewTimestampValue creates a new timestamp value
func NewTimestampValue(value time.Time) ColumnValue {
	v := NewDirectValue(TIMESTAMP)
	v.timeValue = value.UTC()
	v.valueRef = value.UTC() // Store reference to original value
	return v
}

// NewJSONValue creates a new JSON value
func NewJSONValue(value string) ColumnValue {
	v := NewDirectValue(JSON)
	v.stringValue = value
	v.valueRef = value // Store reference to original value
	return v
}

// Type-specific null value creation helpers

// NewNullIntegerValue creates a new NULL integer value
func NewNullIntegerValue() ColumnValue {
	return StaticNullInteger
}

// NewNullFloatValue creates a new NULL float value
func NewNullFloatValue() ColumnValue {
	return StaticNullFloat
}

// NewNullStringValue creates a new NULL string value
func NewNullStringValue() ColumnValue {
	return StaticNullString
}

// NewNullBooleanValue creates a new NULL boolean value
func NewNullBooleanValue() ColumnValue {
	return StaticNullBoolean
}

// NewNullTimestampValue creates a new NULL timestamp value
func NewNullTimestampValue() ColumnValue {
	return StaticNullTimestamp
}

// NewNullJSONValue creates a new NULL JSON value
func NewNullJSONValue() ColumnValue {
	return StaticNullJSON
}

// NewNullValue creates a NULL value of the specified type
func NewNullValue(colType DataType) ColumnValue {
	return NewNullDirectValue(colType)
}
