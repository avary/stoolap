package scalar

import (
	"fmt"
	"math"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// FloorFunction implements the FLOOR function
type FloorFunction struct{}

// Name returns the name of the function
func (f *FloorFunction) Name() string {
	return "FLOOR"
}

// GetInfo returns the function information
func (f *FloorFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "FLOOR",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the largest integer value not greater than the argument",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *FloorFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the floor of a number
func (f *FloorFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	num, err := ConvertToFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid number: %v", err)
	}

	return math.Floor(num), nil
}

// NewFloorFunction creates a new FLOOR function
func NewFloorFunction() contract.ScalarFunction {
	return &FloorFunction{}
}

// Self-registration
func init() {
	// Register the FLOOR function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewFloorFunction())
	}
}
