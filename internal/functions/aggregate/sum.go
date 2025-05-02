package aggregate

import (
	"fmt"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// SumFunction implements the SUM aggregate function
type SumFunction struct {
	sum         float64
	distinct    bool
	values      map[float64]struct{} // used for DISTINCT
	allIntegers bool                 // track if all inputs are integers
	initialized bool                 // track if we've seen any values
	intSum      int64                // separate sum for integer values
}

// Name returns the name of the function
func (f *SumFunction) Name() string {
	return "SUM"
}

// GetInfo returns the function information
func (f *SumFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "SUM",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the sum of all non-NULL values in the specified column. Returns int64 for integer inputs, float64 for floating-point inputs.",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,                          // can return either int64 or float64 based on inputs
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts numeric types
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the SUM function with the registry
func (f *SumFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the SUM calculation
func (f *SumFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (SUM ignores NULLs)
	if value == nil {
		return
	}

	// First try direct type conversion for performance
	var numericValue float64
	var isInteger bool
	var intValue int64
	var err error

	// Handle direct numeric types
	switch v := value.(type) {
	case float64:
		numericValue = v
		isInteger = false
	case float32:
		numericValue = float64(v)
		isInteger = false
	case int:
		intValue = int64(v)
		numericValue = float64(v)
		isInteger = true
	case int64:
		intValue = v
		numericValue = float64(v)
		isInteger = true
	case int32:
		intValue = int64(v)
		numericValue = float64(v)
		isInteger = true
	default:
		// Handle storage engine types with AsXXX methods
		if iVal, ok := value.(interface{ AsInt64() (int64, bool) }); ok {
			if i64, ok := iVal.AsInt64(); ok {
				intValue = i64
				numericValue = float64(i64)
				isInteger = true
				err = nil
			} else {
				err = fmt.Errorf("AsInt64 method failed")
			}
		} else if fVal, ok := value.(interface{ AsFloat64() (float64, bool) }); ok {
			if f64, ok := fVal.AsFloat64(); ok {
				numericValue = f64
				isInteger = false
				err = nil
			} else {
				err = fmt.Errorf("AsFloat64 method failed")
			}
		} else {
			// Fall back to reflection for other types
			numericValue, err = toFloat64(value)
			// Check if this is actually an integer value
			if err == nil && numericValue == float64(int64(numericValue)) {
				intValue = int64(numericValue)
				isInteger = true
			} else {
				isInteger = false
			}
		}
	}

	if err != nil {
		// Skip non-numeric values
		return
	}

	// Initialize tracking for first value
	if !f.initialized {
		f.initialized = true
		f.allIntegers = isInteger
	} else {
		// If we see a non-integer value, update the tracking
		if !isInteger {
			f.allIntegers = false
		}
	}

	// Handle DISTINCT case
	if distinct {
		if f.values == nil {
			f.values = make(map[float64]struct{})
		}

		// Only add values we haven't seen before
		if _, exists := f.values[numericValue]; !exists {
			f.values[numericValue] = struct{}{}
			f.sum += numericValue
			if isInteger {
				f.intSum += intValue
			}
		}
	} else {
		// Regular SUM
		f.sum += numericValue
		if isInteger {
			f.intSum += intValue
		}
	}
}

// Result returns the final result of the SUM calculation
func (f *SumFunction) Result() interface{} {
	// Return int64 for integer inputs, float64 for floating point inputs
	if f.initialized && f.allIntegers {
		return f.intSum
	}

	return f.sum
}

// Reset resets the SUM calculation
func (f *SumFunction) Reset() {
	f.sum = 0
	f.intSum = 0
	f.values = nil
	f.distinct = false
	f.allIntegers = true
	f.initialized = false
}

// NewSumFunction creates a new SUM function
func NewSumFunction() contract.AggregateFunction {
	return &SumFunction{
		sum:         0,
		intSum:      0,
		values:      make(map[float64]struct{}),
		distinct:    false,
		allIntegers: true, // Start assuming all integers until we see a float
		initialized: false,
	}
}

// Self-registration
func init() {
	// Register the SUM function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewSumFunction())
	}
}

// toFloat64 converts a value to float64
func toFloat64(value interface{}) (float64, error) {
	// Handle common types directly without reflection first
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	}

	// Try using type assertions for common v3 storage engine interfaces
	// AsFloat64 method is commonly used
	if floatVal, ok := value.(interface{ AsFloat64() (float64, bool) }); ok {
		if f64, ok := floatVal.AsFloat64(); ok {
			return f64, nil
		}
	}

	// AsInt64 method is commonly used for integer values
	if intVal, ok := value.(interface{ AsInt64() (int64, bool) }); ok {
		if i64, ok := intVal.AsInt64(); ok {
			return float64(i64), nil
		}
	}

	return 0, fmt.Errorf("unsupported type: %T", value)
}
