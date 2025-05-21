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

package aggregate

import (
	"reflect"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// MinFunction implements the MIN aggregate function
type MinFunction struct {
	minValue interface{}
	dataType funcregistry.DataType
	distinct bool // DISTINCT doesn't change MIN behavior, but we track it for consistency
}

// Name returns the name of the function
func (f *MinFunction) Name() string {
	return "MIN"
}

// GetInfo returns the function information
func (f *MinFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "MIN",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the minimum value of all non-NULL values in the specified column",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,                          // MIN returns the same type as the input
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts any comparable type
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the MIN function with the registry
func (f *MinFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the MIN calculation
func (f *MinFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (MIN ignores NULLs)
	if value == nil {
		return
	}

	// If this is the first non-NULL value, set it as the minimum
	if f.minValue == nil {
		f.minValue = value
		f.dataType = getDataType(value)
		return
	}

	// Compare the current value with the minimum
	if isLessThan(value, f.minValue) {
		f.minValue = value
	}
}

// Result returns the final result of the MIN calculation
func (f *MinFunction) Result() interface{} {
	return f.minValue // Returns NULL if no values were accumulated
}

// Reset resets the MIN calculation
func (f *MinFunction) Reset() {
	f.minValue = nil
	f.dataType = funcregistry.TypeUnknown
	f.distinct = false
}

// NewMinFunction creates a new MIN function
func NewMinFunction() contract.AggregateFunction {
	return &MinFunction{
		minValue: nil,
		dataType: funcregistry.TypeUnknown,
		distinct: false,
	}
}

// Self-registration
func init() {
	// Register the MIN function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewMinFunction())
	}
}

// isLessThan compares two values and returns true if a < b
func isLessThan(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return false // equal, not less than
	}
	if a == nil {
		return true // nil is less than any non-nil value
	}
	if b == nil {
		return false // no value is less than nil
	}

	// Handle time.Time specifically (common case)
	if aTime, aOk := a.(time.Time); aOk {
		if bTime, bOk := b.(time.Time); bOk {
			return aTime.Before(bTime)
		}
		// time.Time is considered less than other types
		return true
	} else if _, bOk := b.(time.Time); bOk {
		// Other types are greater than time.Time
		return false
	}

	// Compare numeric types with proper type conversion
	// Integer types
	switch a := a.(type) {
	case int:
		switch b := b.(type) {
		case int:
			return a < b
		case int8:
			return int8(a) < b
		case int16:
			return int16(a) < b
		case int32:
			return int32(a) < b
		case int64:
			return int64(a) < b
		case uint:
			if a < 0 {
				return true
			}
			return uint(a) < b
		case uint8:
			if a < 0 {
				return true
			}
			return uint8(a) < b
		case uint16:
			if a < 0 {
				return true
			}
			return uint16(a) < b
		case uint32:
			if a < 0 {
				return true
			}
			return uint32(a) < b
		case uint64:
			if a < 0 {
				return true
			}
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // ints > bools
		case string:
			return false // ints < strings
		}
	case int8:
		switch b := b.(type) {
		case int:
			return int(a) < b
		case int8:
			return a < b
		case int16:
			return int16(a) < b
		case int32:
			return int32(a) < b
		case int64:
			return int64(a) < b
		case uint:
			if a < 0 {
				return true
			}
			return uint(a) < b
		case uint8:
			if a < 0 {
				return true
			}
			return uint8(a) < b
		case uint16:
			if a < 0 {
				return true
			}
			return uint16(a) < b
		case uint32:
			if a < 0 {
				return true
			}
			return uint32(a) < b
		case uint64:
			if a < 0 {
				return true
			}
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // ints > bools
		case string:
			return false // ints < strings
		}
	case int16:
		switch b := b.(type) {
		case int:
			return int(a) < b
		case int8:
			return a < int16(b)
		case int16:
			return a < b
		case int32:
			return int32(a) < b
		case int64:
			return int64(a) < b
		case uint:
			if a < 0 {
				return true
			}
			return uint(a) < b
		case uint8:
			if a < 0 {
				return true
			}
			return uint8(a) < b
		case uint16:
			if a < 0 {
				return true
			}
			return uint16(a) < b
		case uint32:
			if a < 0 {
				return true
			}
			return uint32(a) < b
		case uint64:
			if a < 0 {
				return true
			}
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // ints > bools
		case string:
			return false // ints < strings
		}
	case int32:
		switch b := b.(type) {
		case int:
			return int(a) < b
		case int8:
			return a < int32(b)
		case int16:
			return a < int32(b)
		case int32:
			return a < b
		case int64:
			return int64(a) < b
		case uint:
			if a < 0 {
				return true
			}
			return uint(a) < b
		case uint8:
			if a < 0 {
				return true
			}
			return uint8(a) < b
		case uint16:
			if a < 0 {
				return true
			}
			return uint16(a) < b
		case uint32:
			if a < 0 {
				return true
			}
			return uint32(a) < b
		case uint64:
			if a < 0 {
				return true
			}
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // ints > bools
		case string:
			return false // ints < strings
		}
	case int64:
		switch b := b.(type) {
		case int:
			return a < int64(b)
		case int8:
			return a < int64(b)
		case int16:
			return a < int64(b)
		case int32:
			return a < int64(b)
		case int64:
			return a < b
		case uint:
			if a < 0 {
				return true
			}
			return uint64(a) < uint64(b)
		case uint8:
			if a < 0 {
				return true
			}
			return uint64(a) < uint64(b)
		case uint16:
			if a < 0 {
				return true
			}
			return uint64(a) < uint64(b)
		case uint32:
			if a < 0 {
				return true
			}
			return uint64(a) < uint64(b)
		case uint64:
			if a < 0 {
				return true
			}
			return uint64(a) < b
		case float32:
			return float64(a) < float64(b)
		case float64:
			return float64(a) < b
		case bool:
			return false // ints > bools
		case string:
			return false // ints < strings
		}
	// Unsigned integer types
	case uint:
		switch b := b.(type) {
		case int:
			if b < 0 {
				return false
			}
			return a < uint(b)
		case int8:
			if b < 0 {
				return false
			}
			return a < uint(b)
		case int16:
			if b < 0 {
				return false
			}
			return a < uint(b)
		case int32:
			if b < 0 {
				return false
			}
			return a < uint(b)
		case int64:
			if b < 0 {
				return false
			}
			return uint64(a) < uint64(b)
		case uint:
			return a < b
		case uint8:
			return a < uint(b)
		case uint16:
			return a < uint(b)
		case uint32:
			return a < uint(b)
		case uint64:
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // uints > bools
		case string:
			return false // uints < strings
		}
	case uint8:
		switch b := b.(type) {
		case int:
			if b < 0 {
				return false
			}
			return a < uint8(b)
		case int8:
			if b < 0 {
				return false
			}
			return a < uint8(b)
		case int16:
			if b < 0 {
				return false
			}
			return a < uint8(b)
		case int32:
			if b < 0 {
				return false
			}
			return a < uint8(b)
		case int64:
			if b < 0 {
				return false
			}
			return uint64(a) < uint64(b)
		case uint:
			return uint(a) < b
		case uint8:
			return a < b
		case uint16:
			return uint16(a) < b
		case uint32:
			return uint32(a) < b
		case uint64:
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // uints > bools
		case string:
			return false // uints < strings
		}
	case uint16:
		switch b := b.(type) {
		case int:
			if b < 0 {
				return false
			}
			return a < uint16(b)
		case int8:
			if b < 0 {
				return false
			}
			return a < uint16(b)
		case int16:
			if b < 0 {
				return false
			}
			return a < uint16(b)
		case int32:
			if b < 0 {
				return false
			}
			return a < uint16(b)
		case int64:
			if b < 0 {
				return false
			}
			return uint64(a) < uint64(b)
		case uint:
			return uint(a) < b
		case uint8:
			return a < uint16(b)
		case uint16:
			return a < b
		case uint32:
			return uint32(a) < b
		case uint64:
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // uints > bools
		case string:
			return false // uints < strings
		}
	case uint32:
		switch b := b.(type) {
		case int:
			if b < 0 {
				return false
			}
			return a < uint32(b)
		case int8:
			if b < 0 {
				return false
			}
			return a < uint32(b)
		case int16:
			if b < 0 {
				return false
			}
			return a < uint32(b)
		case int32:
			if b < 0 {
				return false
			}
			return a < uint32(b)
		case int64:
			if b < 0 {
				return false
			}
			return uint64(a) < uint64(b)
		case uint:
			return uint(a) < b
		case uint8:
			return a < uint32(b)
		case uint16:
			return a < uint32(b)
		case uint32:
			return a < b
		case uint64:
			return uint64(a) < b
		case float32:
			return float32(a) < b
		case float64:
			return float64(a) < b
		case bool:
			return false // uints > bools
		case string:
			return false // uints < strings
		}
	case uint64:
		switch b := b.(type) {
		case int:
			if b < 0 {
				return false
			}
			return a < uint64(b)
		case int8:
			if b < 0 {
				return false
			}
			return a < uint64(b)
		case int16:
			if b < 0 {
				return false
			}
			return a < uint64(b)
		case int32:
			if b < 0 {
				return false
			}
			return a < uint64(b)
		case int64:
			if b < 0 {
				return false
			}
			return a < uint64(b)
		case uint:
			return a < uint64(b)
		case uint8:
			return a < uint64(b)
		case uint16:
			return a < uint64(b)
		case uint32:
			return a < uint64(b)
		case uint64:
			return a < b
		case float32:
			return float64(a) < float64(b)
		case float64:
			return float64(a) < b
		case bool:
			return false // uints > bools
		case string:
			return false // uints < strings
		}
	// Floating-point types
	case float32:
		switch b := b.(type) {
		case int:
			return a < float32(b)
		case int8:
			return a < float32(b)
		case int16:
			return a < float32(b)
		case int32:
			return a < float32(b)
		case int64:
			return float64(a) < float64(b)
		case uint:
			return a < float32(b)
		case uint8:
			return a < float32(b)
		case uint16:
			return a < float32(b)
		case uint32:
			return a < float32(b)
		case uint64:
			return float64(a) < float64(b)
		case float32:
			return a < b
		case float64:
			return float64(a) < b
		case bool:
			return false // floats > bools
		case string:
			return false // floats < strings
		}
	case float64:
		switch b := b.(type) {
		case int:
			return a < float64(b)
		case int8:
			return a < float64(b)
		case int16:
			return a < float64(b)
		case int32:
			return a < float64(b)
		case int64:
			return a < float64(b)
		case uint:
			return a < float64(b)
		case uint8:
			return a < float64(b)
		case uint16:
			return a < float64(b)
		case uint32:
			return a < float64(b)
		case uint64:
			return a < float64(b)
		case float32:
			return a < float64(b)
		case float64:
			return a < b
		case bool:
			return false // floats > bools
		case string:
			return false // floats < strings
		}
	// Other basic types
	case bool:
		switch b := b.(type) {
		case bool:
			// false < true
			return !a && b
		default:
			// bools come before other types (except time.Time)
			return true
		}
	case string:
		switch b := b.(type) {
		case string:
			return strings.Compare(a, b) < 0
		case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			// strings come after numeric and bool types
			return false
		default:
			// strings come before other complex types
			return true
		}
	}

	// For different types that don't have specific comparisons,
	// we use type name string comparison for a stable order
	aType := typeNameOf(a)
	bType := typeNameOf(b)
	if aType != bType {
		return aType < bType
	}

	// Final fallback - should rarely get here
	return false
}

// typeNameOf returns the type name as a string without using fmt.Sprintf
func typeNameOf(v interface{}) string {
	if v == nil {
		return "nil"
	}

	switch v.(type) {
	case bool:
		return "bool"
	case int:
		return "int"
	case int8:
		return "int8"
	case int16:
		return "int16"
	case int32:
		return "int32"
	case int64:
		return "int64"
	case uint:
		return "uint"
	case uint8:
		return "uint8"
	case uint16:
		return "uint16"
	case uint32:
		return "uint32"
	case uint64:
		return "uint64"
	case float32:
		return "float32"
	case float64:
		return "float64"
	case string:
		return "string"
	case time.Time:
		return "time.Time"
	}

	// For custom types, use reflection as a last resort
	// This is only used for the fallback case
	return reflect.TypeOf(v).String()
}

// getDataType determines the data type of a value
func getDataType(value interface{}) funcregistry.DataType {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return funcregistry.TypeInteger
	case reflect.Float32, reflect.Float64:
		return funcregistry.TypeFloat
	case reflect.String:
		return funcregistry.TypeString
	case reflect.Bool:
		return funcregistry.TypeBoolean
	default:
		return funcregistry.TypeAny
	}
}
