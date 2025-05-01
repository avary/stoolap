package scalar

import (
	"fmt"
	"math"

	"github.com/semihalev/stoolap/internal/functions/contract"
	"github.com/semihalev/stoolap/internal/functions/registry"
	"github.com/semihalev/stoolap/internal/parser/funcregistry"
)

// AbsFunction implements the ABS function
type AbsFunction struct{}

// Name returns the name of the function
func (f *AbsFunction) Name() string {
	return "ABS"
}

// GetInfo returns the function information
func (f *AbsFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "ABS",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the absolute value of a number",
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
func (f *AbsFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the absolute value of a number
func (f *AbsFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	num, err := ConvertToFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid number: %v", err)
	}

	return math.Abs(num), nil
}

// NewAbsFunction creates a new ABS function
func NewAbsFunction() contract.ScalarFunction {
	return &AbsFunction{}
}

// Self-registration
func init() {
	// Register the ABS function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewAbsFunction())
	}
}
