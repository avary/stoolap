package scalar

import (
	"fmt"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// LengthFunction implements the LENGTH function
type LengthFunction struct{}

// Name returns the name of the function
func (f *LengthFunction) Name() string {
	return "LENGTH"
}

// GetInfo returns the function information
func (f *LengthFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "LENGTH",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the length of a string",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeInteger,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *LengthFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the length of a string
func (f *LengthFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	str := ConvertToString(args[0])
	return int64(len(str)), nil
}

// NewLengthFunction creates a new LENGTH function
func NewLengthFunction() contract.ScalarFunction {
	return &LengthFunction{}
}

// Self-registration
func init() {
	// Register the LENGTH function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewLengthFunction())
	}
}
