package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// -----------------------------------------------------
// ColumnValue Pooling System
// -----------------------------------------------------

// Pool for each type of DirectValue
var (
	// IntegerValuePool is a pool for integer values
	integerValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: INTEGER}
		},
	}

	// FloatValuePool is a pool for float values
	floatValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: FLOAT}
		},
	}

	// StringValuePool is a pool for string values
	stringValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: TEXT}
		},
	}

	// BooleanValuePool is a pool for boolean values
	booleanValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: BOOLEAN}
		},
	}

	// TimestampValuePool is a pool for timestamp values
	timestampValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: TIMESTAMP}
		},
	}

	// DateValuePool is a pool for date values
	dateValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: DATE}
		},
	}

	// TimeValuePool is a pool for time values
	timeValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: TIME}
		},
	}

	// JSONValuePool is a pool for JSON values
	jsonValuePool = sync.Pool{
		New: func() interface{} {
			return &DirectValue{dataType: JSON}
		},
	}
)

// GetPooledColumnValue gets a pooled value of the specified type
func GetPooledColumnValue(val interface{}) ColumnValue {
	if val == nil {
		return StaticNullUnknown
	}

	// Handle common special cases with static precomputed values
	switch v := val.(type) {
	case int64:
		if v == 0 {
			return staticZeroInt
		} else if v == 1 {
			return staticOneInt
		}
	case int:
		if v == 0 {
			return staticZeroInt
		} else if v == 1 {
			return staticOneInt
		}
	case float64:
		if v == 0.0 {
			return staticZeroFloat
		} else if v == 1.0 {
			return staticOneFloat
		}
	case string:
		if v == "" {
			return staticEmptyString
		}
	case bool:
		if v {
			return staticBooleanTrue
		}
		return staticBooleanFalse
	}

	// Get pooled value based on type
	switch v := val.(type) {
	case int:
		obj := integerValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.intValue = int64(v)
		obj.valueRef = v
		obj.isNull = false
		return obj
	case int64:
		obj := integerValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.intValue = v
		obj.valueRef = v
		obj.isNull = false
		return obj
	case float32:
		obj := floatValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.floatValue = float64(v)
		obj.valueRef = float64(v)
		obj.isNull = false
		return obj
	case float64:
		obj := floatValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.floatValue = v
		obj.valueRef = v
		obj.isNull = false
		return obj
	case string:
		obj := stringValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.stringValue = v
		obj.valueRef = v
		obj.isNull = false
		return obj
	case bool:
		// Boolean values are handled by static instances
		if v {
			return staticBooleanTrue
		}
		return staticBooleanFalse
	case time.Time:
		// Determine if it's a date, time, or timestamp based on zero values
		hasTime := v.Hour() != 0 || v.Minute() != 0 || v.Second() != 0 || v.Nanosecond() != 0
		hasDate := v.Year() != 1 || v.Month() != 1 || v.Day() != 1

		if hasDate && hasTime {
			obj := timestampValuePool.Get().(*DirectValue)
			obj.pooled.Store(true)
			obj.timeValue = v
			obj.valueRef = v
			obj.isNull = false
			return obj
		} else if hasDate {
			obj := dateValuePool.Get().(*DirectValue)
			obj.pooled.Store(true)
			obj.timeValue = NormalizeDate(v)
			obj.valueRef = obj.timeValue
			obj.isNull = false
			return obj
		} else if hasTime {
			obj := timeValuePool.Get().(*DirectValue)
			obj.pooled.Store(true)
			obj.timeValue = NormalizeTime(v)
			obj.valueRef = obj.timeValue
			obj.isNull = false
			return obj
		} else {
			// Default to timestamp for empty time
			obj := timestampValuePool.Get().(*DirectValue)
			obj.pooled.Store(true)
			obj.timeValue = v
			obj.valueRef = v
			obj.isNull = false
			return obj
		}
	case []byte:
		// Try to parse as JSON
		var jsonObj interface{}
		if json.Unmarshal(v, &jsonObj) == nil {
			obj := jsonValuePool.Get().(*DirectValue)
			obj.pooled.Store(true)
			obj.stringValue = string(v)
			obj.valueRef = jsonObj
			obj.isNull = false
			return obj
		}
		// Fall back to string
		obj := stringValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.stringValue = string(v)
		obj.valueRef = obj.stringValue
		obj.isNull = false
		return obj
	case map[string]interface{}, []interface{}:
		// JSON object
		jsonBytes, _ := json.Marshal(v)
		obj := jsonValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.stringValue = string(jsonBytes)
		obj.valueRef = v
		obj.isNull = false
		return obj
	default:
		// Fall back to string representation
		str := fmt.Sprintf("%v", v)
		obj := stringValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.stringValue = str
		obj.valueRef = str
		obj.isNull = false
		return obj
	}
}

// GetPooledIntegerValue gets a pooled integer value
func GetPooledIntegerValue(val int64) ColumnValue {
	// Use static values for common cases
	if val == 0 {
		return staticZeroInt
	} else if val == 1 {
		return staticOneInt
	}
	obj := integerValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.intValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// GetPooledFloatValue gets a pooled float value
func GetPooledFloatValue(val float64) ColumnValue {
	// Use static values for common cases
	if val == 0.0 {
		return staticZeroFloat
	} else if val == 1.0 {
		return staticOneFloat
	}
	obj := floatValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.floatValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// GetPooledStringValue gets a pooled string value
func GetPooledStringValue(val string) ColumnValue {
	// Use static value for empty string
	if val == "" {
		return staticEmptyString
	}
	obj := stringValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.stringValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// GetPooledBooleanValue gets a pooled boolean value
func GetPooledBooleanValue(val bool) ColumnValue {
	// Use static boolean values
	if val {
		return staticBooleanTrue
	}
	return staticBooleanFalse
}

// GetPooledTimestampValue gets a pooled timestamp value
func GetPooledTimestampValue(val time.Time) ColumnValue {
	obj := timestampValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.timeValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// GetPooledDateValue gets a pooled date value
func GetPooledDateValue(val time.Time) ColumnValue {
	obj := dateValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.timeValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// GetPooledTimeValue gets a pooled time value
func GetPooledTimeValue(val time.Time) ColumnValue {
	// Always normalize to year 1 for consistent behavior
	normalizedTime := time.Date(1, 1, 1, val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), time.UTC)
	obj := timeValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.timeValue = normalizedTime
	obj.valueRef = normalizedTime
	obj.isNull = false
	return obj
}

// GetPooledJSONValue gets a pooled JSON value
func GetPooledJSONValue(val string) ColumnValue {
	obj := jsonValuePool.Get().(*DirectValue)
	obj.pooled.Store(true)
	obj.stringValue = val
	obj.valueRef = val
	obj.isNull = false
	return obj
}

// PutPooledColumnValue returns a value to its appropriate pool
func PutPooledColumnValue(val ColumnValue) {
	// Only DirectValue objects can be returned to the pool
	dv, ok := val.(*DirectValue)
	if !ok || dv == nil || dv.isNull {
		return // Do not pool NULL values or non-DirectValue objects
	}

	if !dv.pooled.Load() {
		return // Do not pool if not pooled
	}

	// Check for static values that should not be returned to the pool
	if val == staticBooleanTrue || val == staticBooleanFalse ||
		val == staticZeroInt || val == staticOneInt ||
		val == staticZeroFloat || val == staticOneFloat ||
		val == staticEmptyString {
		return // Do not pool static values
	}

	// Reset common fields
	dv.isNull = false

	dv.pooled.Store(false)

	// Return to the appropriate pool based on type
	switch dv.dataType {
	case INTEGER:
		dv.floatValue = 0 // Clear other fields
		dv.stringValue = ""
		dv.boolValue = false
		dv.timeValue = time.Time{}
		integerValuePool.Put(dv)
	case FLOAT:
		dv.intValue = 0 // Clear other fields
		dv.stringValue = ""
		dv.boolValue = false
		dv.timeValue = time.Time{}
		floatValuePool.Put(dv)
	case TEXT:
		dv.intValue = 0 // Clear other fields
		dv.floatValue = 0
		dv.boolValue = false
		dv.timeValue = time.Time{}
		stringValuePool.Put(dv)
	case BOOLEAN:
		// We use static boolean values, so this should never happen
		// Just in case, reset and return
		dv.intValue = 0
		dv.floatValue = 0
		dv.stringValue = ""
		dv.timeValue = time.Time{}
		booleanValuePool.Put(dv)
	case TIMESTAMP:
		dv.intValue = 0
		dv.floatValue = 0
		dv.stringValue = ""
		dv.boolValue = false
		timestampValuePool.Put(dv)
	case DATE:
		dv.intValue = 0
		dv.floatValue = 0
		dv.stringValue = ""
		dv.boolValue = false
		dateValuePool.Put(dv)
	case TIME:
		dv.intValue = 0
		dv.floatValue = 0
		dv.stringValue = ""
		dv.boolValue = false
		timeValuePool.Put(dv)
	case JSON:
		dv.intValue = 0
		dv.floatValue = 0
		dv.boolValue = false
		dv.timeValue = time.Time{}
		jsonValuePool.Put(dv)
	}
}
