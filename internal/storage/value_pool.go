/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
		obj := timestampValuePool.Get().(*DirectValue)
		obj.pooled.Store(true)
		obj.timeValue = v.UTC()
		obj.valueRef = v.UTC()
		obj.isNull = false
		return obj
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
	obj.timeValue = val.UTC()
	obj.valueRef = val.UTC()
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
	case JSON:
		dv.intValue = 0
		dv.floatValue = 0
		dv.boolValue = false
		dv.timeValue = time.Time{}
		jsonValuePool.Put(dv)
	}
}
