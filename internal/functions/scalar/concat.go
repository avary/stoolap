package scalar

import (
	"strings"

	"github.com/semihalev/stoolap/internal/functions/contract"
	"github.com/semihalev/stoolap/internal/functions/registry"
	"github.com/semihalev/stoolap/internal/parser/funcregistry"
)

// ConcatFunction implements the CONCAT function
type ConcatFunction struct{}

// Name returns the name of the function
func (f *ConcatFunction) Name() string {
	return "CONCAT"
}

// GetInfo returns the function information
func (f *ConcatFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "CONCAT",
		Type:        funcregistry.ScalarFunction,
		Description: "Concatenates strings",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       -1, // unlimited
			IsVariadic:    true,
		},
	}
}

// Register registers the function with the registry
func (f *ConcatFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate concatenates strings
func (f *ConcatFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}

	var sb strings.Builder
	for _, arg := range args {
		if arg != nil {
			sb.WriteString(ConvertToString(arg))
		}
	}

	return sb.String(), nil
}

// NewConcatFunction creates a new CONCAT function
func NewConcatFunction() contract.ScalarFunction {
	return &ConcatFunction{}
}

// Self-registration
func init() {
	// Register the CONCAT function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewConcatFunction())
	}
}
