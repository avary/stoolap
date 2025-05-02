package scalar

import (
	"fmt"
	"strings"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// UpperFunction implements the UPPER function
type UpperFunction struct{}

// Name returns the name of the function
func (f *UpperFunction) Name() string {
	return "UPPER"
}

// GetInfo returns the function information
func (f *UpperFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "UPPER",
		Type:        funcregistry.ScalarFunction,
		Description: "Converts a string to uppercase",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *UpperFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate converts the string to uppercase
func (f *UpperFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	str := ConvertToString(args[0])
	return strings.ToUpper(str), nil
}

// NewUpperFunction creates a new UPPER function
func NewUpperFunction() contract.ScalarFunction {
	return &UpperFunction{}
}

// Self-registration
func init() {
	// Register the UPPER function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewUpperFunction())
	}
}
