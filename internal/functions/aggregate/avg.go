package aggregate

import (
	"fmt"

	"github.com/semihalev/stoolap/internal/functions/contract"
	"github.com/semihalev/stoolap/internal/functions/registry"
	"github.com/semihalev/stoolap/internal/parser/funcregistry"
)

// AvgFunction implements the AVG aggregate function
type AvgFunction struct {
	sum      float64
	count    int64
	distinct bool
	values   map[float64]struct{} // used for DISTINCT
}

// Name returns the name of the function
func (f *AvgFunction) Name() string {
	return "AVG"
}

// GetInfo returns the function information
func (f *AvgFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "AVG",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the average of all non-NULL values in the specified column",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts numeric types
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the AVG function with the registry
func (f *AvgFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the AVG calculation
func (f *AvgFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (AVG ignores NULLs)
	if value == nil {
		return
	}

	// First try direct type conversion for performance
	var numericValue float64
	var err error

	// Handle direct numeric types
	switch v := value.(type) {
	case float64:
		numericValue = v
	case float32:
		numericValue = float64(v)
	case int:
		numericValue = float64(v)
	case int64:
		numericValue = float64(v)
	case int32:
		numericValue = float64(v)
	default:
		// Handle storage engine types with AsXXX methods
		if fVal, ok := value.(interface{ AsFloat64() (float64, bool) }); ok {
			if f64, ok := fVal.AsFloat64(); ok {
				numericValue = f64
				err = nil
			} else {
				err = fmt.Errorf("AsFloat64 method failed")
			}
		} else if iVal, ok := value.(interface{ AsInt64() (int64, bool) }); ok {
			if i64, ok := iVal.AsInt64(); ok {
				numericValue = float64(i64)
				err = nil
			} else {
				err = fmt.Errorf("AsInt64 method failed")
			}
		} else {
			// Fall back to reflection for other types
			numericValue, err = toFloat64(value)
		}
	}

	if err != nil {
		// Skip non-numeric values
		return
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
			f.count++
		}
	} else {
		// Regular AVG
		f.sum += numericValue
		f.count++
	}
}

// Result returns the final result of the AVG calculation
func (f *AvgFunction) Result() interface{} {
	if f.count == 0 {
		return nil // Return NULL for empty sets
	}
	return f.sum / float64(f.count)
}

// Reset resets the AVG calculation
func (f *AvgFunction) Reset() {
	f.sum = 0
	f.count = 0
	f.values = nil
	f.distinct = false
}

// NewAvgFunction creates a new AVG function
func NewAvgFunction() contract.AggregateFunction {
	return &AvgFunction{
		sum:      0,
		count:    0,
		values:   make(map[float64]struct{}),
		distinct: false,
	}
}

// Self-registration
func init() {
	// Register the AVG function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewAvgFunction())
	}
}
