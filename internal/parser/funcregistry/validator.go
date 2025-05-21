/* Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package funcregistry

import (
	"fmt"
	"strings"
)

// Validator is responsible for validating function calls
// and populating function info in the AST
type Validator struct {
	registry Registry
}

// NewValidator creates a new function validator
func NewValidator(registry Registry) *Validator {
	return &Validator{
		registry: registry,
	}
}

// ExpressionType represents the data type of an expression
type ExpressionType interface {
	// GetType returns the data type of the expression
	GetType() DataType
}

// ValidateFunctionCall validates a function call
// It populates the FunctionInfo field of the provided function call info
func (v *Validator) ValidateFunctionCall(functionName string, infoPtr **FunctionInfo, argTypes []DataType) error {
	// Function names are case-insensitive in SQL
	functionName = strings.ToUpper(functionName)

	// Get function information from registry
	info, err := v.registry.Get(functionName)
	if err != nil {
		return err
	}

	// Validate argument types
	if err := info.Signature.ValidateArgs(argTypes); err != nil {
		return err
	}

	// Update the function info in the caller
	*infoPtr = &info

	return nil
}

// GetKnownDataType tries to determine the data type of a value
func GetKnownDataType(value interface{}) DataType {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return TypeInteger
	case float32, float64:
		return TypeFloat
	case string:
		return TypeString
	case bool:
		return TypeBoolean
	case nil:
		return TypeUnknown
	default:
		// Try to get more specific types
		typeStr := fmt.Sprintf("%T", v)

		if strings.Contains(typeStr, "Time") ||
			strings.Contains(typeStr, "Date") {
			return TypeDateTime
		}

		if strings.Contains(typeStr, "map") ||
			strings.Contains(typeStr, "struct") {
			return TypeJSON
		}

		if strings.Contains(typeStr, "slice") ||
			strings.Contains(typeStr, "array") {
			return TypeArray
		}

		return TypeUnknown
	}
}

// DetermineLiteralType determines the type of a literal value
// This is a simple implementation; in a real system you would want more nuanced type detection
func DetermineLiteralType(literals map[string]interface{}) map[string]DataType {
	result := make(map[string]DataType)

	for name, value := range literals {
		result[name] = GetKnownDataType(value)
	}

	return result
}

// TypeOf returns the data type of an expression type
func TypeOf(expr ExpressionType) DataType {
	if expr == nil {
		return TypeUnknown
	}
	return expr.GetType()
}
