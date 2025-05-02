package scalar

import (
	"fmt"
	"math"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// CeilingFunction implements the CEILING/CEIL function
type CeilingFunction struct{}

// Name returns the name of the function
func (f *CeilingFunction) Name() string {
	return "CEILING"
}

// GetInfo returns the function information
func (f *CeilingFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "CEILING",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the smallest integer value not less than the argument",
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
func (f *CeilingFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)

	// Register CEIL as an alias for CEILING
	aliasInfo := info
	aliasInfo.Name = "CEIL"
	registry.MustRegister(aliasInfo)
}

// Evaluate returns the ceiling of a number
func (f *CeilingFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEILING requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	num, err := ConvertToFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid number: %v", err)
	}

	return math.Ceil(num), nil
}

// NewCeilingFunction creates a new CEILING function
func NewCeilingFunction() contract.ScalarFunction {
	return &CeilingFunction{}
}

// Self-registration
func init() {
	// Register the CEILING function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewCeilingFunction())
	}
}
