package aggregate

import (
	"fmt"
	"reflect"
	"strings"

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
	// Get reflection values
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	// Handle different types (numeric types are comparable across types)
	switch va.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Convert both to int64 for comparison
		aInt := va.Int()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return aInt < vb.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if aInt < 0 {
				return true // Negative int is less than any uint
			}
			return uint64(aInt) < vb.Uint()
		case reflect.Float32, reflect.Float64:
			return float64(aInt) < vb.Float()
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Convert both to uint64 for comparison
		aUint := va.Uint()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			bInt := vb.Int()
			if bInt < 0 {
				return false // Any uint is greater than negative int
			}
			return aUint < uint64(bInt)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return aUint < vb.Uint()
		case reflect.Float32, reflect.Float64:
			return float64(aUint) < vb.Float()
		}

	case reflect.Float32, reflect.Float64:
		// Convert both to float64 for comparison
		aFloat := va.Float()

		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return aFloat < float64(vb.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return aFloat < float64(vb.Uint())
		case reflect.Float32, reflect.Float64:
			return aFloat < vb.Float()
		}

	case reflect.String:
		// String comparison
		if vb.Kind() == reflect.String {
			return strings.Compare(va.String(), vb.String()) < 0
		}
	}

	// If types are incomparable, use string representation as a fallback
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
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
