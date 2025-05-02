package aggregate

import (
	"fmt"
	"reflect"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// MaxFunction implements the MAX aggregate function
type MaxFunction struct {
	maxValue interface{}
	dataType funcregistry.DataType
	distinct bool // DISTINCT doesn't change MAX behavior, but we track it for consistency
}

// Name returns the name of the function
func (f *MaxFunction) Name() string {
	return "MAX"
}

// GetInfo returns the function information
func (f *MaxFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "MAX",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the maximum value of all non-NULL values in the specified column",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,                          // MAX returns the same type as the input
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts any comparable type
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the MAX function with the registry
func (f *MaxFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the MAX calculation
func (f *MaxFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (MAX ignores NULLs)
	if value == nil {
		return
	}

	// If this is the first non-NULL value, set it as the maximum
	if f.maxValue == nil {
		f.maxValue = value
		f.dataType = getDataType(value)
		return
	}

	// Compare the current value with the maximum (use isGreaterThan, which is !isLessThan && not equal)
	if !isLessThan(value, f.maxValue) && !isEqual(value, f.maxValue) {
		f.maxValue = value
	}
}

// Result returns the final result of the MAX calculation
func (f *MaxFunction) Result() interface{} {
	return f.maxValue // Returns NULL if no values were accumulated
}

// Reset resets the MAX calculation
func (f *MaxFunction) Reset() {
	f.maxValue = nil
	f.dataType = funcregistry.TypeUnknown
	f.distinct = false
}

// NewMaxFunction creates a new MAX function
func NewMaxFunction() contract.AggregateFunction {
	return &MaxFunction{
		maxValue: nil,
		dataType: funcregistry.TypeUnknown,
		distinct: false,
	}
}

// Self-registration
func init() {
	// Register the MAX function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewMaxFunction())
	}
}

// isEqual compares two values for equality
func isEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Get reflection values
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	// Handle different types
	switch va.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := va.Int()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return aInt == vb.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if aInt < 0 {
				return false // Negative int can't equal unsigned int
			}
			return uint64(aInt) == vb.Uint()
		case reflect.Float32, reflect.Float64:
			return float64(aInt) == vb.Float()
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aUint := va.Uint()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			bInt := vb.Int()
			if bInt < 0 {
				return false // Unsigned int can't equal negative int
			}
			return aUint == uint64(bInt)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return aUint == vb.Uint()
		case reflect.Float32, reflect.Float64:
			return float64(aUint) == vb.Float()
		}

	case reflect.Float32, reflect.Float64:
		aFloat := va.Float()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return aFloat == float64(vb.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return aFloat == float64(vb.Uint())
		case reflect.Float32, reflect.Float64:
			return aFloat == vb.Float()
		}

	case reflect.String:
		// String comparison
		if vb.Kind() == reflect.String {
			return va.String() == vb.String()
		}

	case reflect.Bool:
		// Boolean comparison
		if vb.Kind() == reflect.Bool {
			return va.Bool() == vb.Bool()
		}
	}

	// If we can't compare directly, use string representation
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
