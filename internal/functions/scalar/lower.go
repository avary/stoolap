package scalar

import (
	"fmt"
	"strings"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// LowerFunction implements the LOWER function
type LowerFunction struct{}

// Name returns the name of the function
func (f *LowerFunction) Name() string {
	return "LOWER"
}

// GetInfo returns the function information
func (f *LowerFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "LOWER",
		Type:        funcregistry.ScalarFunction,
		Description: "Converts a string to lowercase",
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
func (f *LowerFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate converts the string to lowercase
func (f *LowerFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	str := ConvertToString(args[0])
	return strings.ToLower(str), nil
}

// NewLowerFunction creates a new LOWER function
func NewLowerFunction() contract.ScalarFunction {
	return &LowerFunction{}
}

// Self-registration
func init() {
	// Register the LOWER function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewLowerFunction())
	}
}
