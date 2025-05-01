package scalar

import (
	"time"

	"github.com/semihalev/stoolap/internal/functions/contract"
	"github.com/semihalev/stoolap/internal/functions/registry"
	"github.com/semihalev/stoolap/internal/parser/funcregistry"
)

// NowFunction implements the NOW() function
type NowFunction struct{}

// Name returns the name of the function
func (f *NowFunction) Name() string {
	return "NOW"
}

// GetInfo returns the function information
func (f *NowFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "NOW",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the current date and time",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeDateTime,
			ArgumentTypes: []funcregistry.DataType{},
			MinArgs:       0,
			MaxArgs:       0,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *NowFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the current date and time
func (f *NowFunction) Evaluate(args ...interface{}) (interface{}, error) {
	return time.Now().Format(time.RFC3339Nano), nil
}

// NewNowFunction creates a new NOW function
func NewNowFunction() contract.ScalarFunction {
	return &NowFunction{}
}

// Self-registration
func init() {
	// Register the NOW function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewNowFunction())
	}
}
