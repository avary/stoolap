package aggregate

import (
	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// CountFunction implements the COUNT aggregate function
type CountFunction struct {
	count    int64
	distinct bool
	values   map[interface{}]struct{} // used for DISTINCT
}

// Name returns the name of the function
func (f *CountFunction) Name() string {
	return "COUNT"
}

// GetInfo returns the function information
func (f *CountFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "COUNT",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the number of rows matching the query criteria",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeInteger,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       0, // COUNT(*) has no actual argument
			MaxArgs:       1, // But can be COUNT(column) or COUNT(DISTINCT column)
			IsVariadic:    false,
		},
	}
}

// Register registers the COUNT function with the registry
func (f *CountFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the COUNT calculation
func (f *CountFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (COUNT ignores NULLs except for COUNT(*))
	if value == nil {
		return
	}

	// Special case for COUNT(*) which counts rows, not values
	if value == "*" {
		f.count++
		return
	}

	// Handle DISTINCT case
	if distinct {
		if f.values == nil {
			f.values = make(map[interface{}]struct{})
		}
		f.values[value] = struct{}{}
	} else {
		// Regular COUNT
		f.count++
	}
}

// Result returns the final result of the COUNT calculation
func (f *CountFunction) Result() interface{} {
	if f.distinct && f.values != nil {
		return int64(len(f.values))
	}
	return f.count
}

// Reset resets the COUNT calculation
func (f *CountFunction) Reset() {
	f.count = 0
	f.values = nil
	f.distinct = false
}

// NewCountFunction creates a new COUNT function
func NewCountFunction() contract.AggregateFunction {
	return &CountFunction{
		count:    0,
		values:   make(map[interface{}]struct{}),
		distinct: false,
	}
}

// Self-registration
func init() {
	// Register the COUNT function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewCountFunction())
	}
}
