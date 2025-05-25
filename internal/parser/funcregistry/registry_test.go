/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package funcregistry

import (
	"testing"
)

func TestFunctionRegistry(t *testing.T) {
	// Create a new registry
	registry := NewRegistry()

	// Register the test functions for this test
	registerTestFunctions(registry)

	// Check that built-in functions are registered
	testCases := []struct {
		name     string
		expected bool
		funcType FunctionType
	}{
		{"COUNT", true, AggregateFunction},
		{"SUM", true, AggregateFunction},
		{"AVG", true, AggregateFunction},
		{"MIN", true, AggregateFunction},
		{"MAX", true, AggregateFunction},
		{"UPPER", true, ScalarFunction},
		{"LOWER", true, ScalarFunction},
		{"LENGTH", true, ScalarFunction},
		{"SUBSTRING", true, ScalarFunction},
		{"ABS", true, ScalarFunction},
		{"ROUND", true, ScalarFunction},
		{"NONEXISTENT", false, ScalarFunction},
	}

	for _, tc := range testCases {
		info, err := registry.Get(tc.name)
		if tc.expected {
			if err != nil {
				t.Errorf("Expected function %s to be registered, but got error: %v", tc.name, err)
			} else if info.Type != tc.funcType {
				t.Errorf("Expected function %s to be of type %v, but got %v", tc.name, tc.funcType, info.Type)
			}
		} else {
			if err == nil {
				t.Errorf("Expected function %s to not be registered", tc.name)
			}
		}
	}
}

// Helper function to register test functions
func registerTestFunctions(registry Registry) {
	// Register aggregate functions
	registry.MustRegister(FunctionInfo{
		Name:        "COUNT",
		Type:        AggregateFunction,
		Description: "Counts the number of rows",
		Signature: FunctionSignature{
			ReturnType:    TypeInteger,
			ArgumentTypes: []DataType{TypeAny}, // COUNT(*) or COUNT(expr)
			MinArgs:       0,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "SUM",
		Type:        AggregateFunction,
		Description: "Calculates the sum of values",
		Signature: FunctionSignature{
			ReturnType:    TypeFloat,
			ArgumentTypes: []DataType{TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "AVG",
		Type:        AggregateFunction,
		Description: "Calculates the average of values",
		Signature: FunctionSignature{
			ReturnType:    TypeFloat,
			ArgumentTypes: []DataType{TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "MIN",
		Type:        AggregateFunction,
		Description: "Finds the minimum value",
		Signature: FunctionSignature{
			ReturnType:    TypeAny,
			ArgumentTypes: []DataType{TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "MAX",
		Type:        AggregateFunction,
		Description: "Finds the maximum value",
		Signature: FunctionSignature{
			ReturnType:    TypeAny,
			ArgumentTypes: []DataType{TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	// Register scalar functions
	registry.MustRegister(FunctionInfo{
		Name:        "UPPER",
		Type:        ScalarFunction,
		Description: "Converts a string to uppercase",
		Signature: FunctionSignature{
			ReturnType:    TypeString,
			ArgumentTypes: []DataType{TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "LOWER",
		Type:        ScalarFunction,
		Description: "Converts a string to lowercase",
		Signature: FunctionSignature{
			ReturnType:    TypeString,
			ArgumentTypes: []DataType{TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "LENGTH",
		Type:        ScalarFunction,
		Description: "Returns the length of a string",
		Signature: FunctionSignature{
			ReturnType:    TypeInteger,
			ArgumentTypes: []DataType{TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "SUBSTRING",
		Type:        ScalarFunction,
		Description: "Returns a substring",
		Signature: FunctionSignature{
			ReturnType:    TypeString,
			ArgumentTypes: []DataType{TypeString, TypeInteger, TypeInteger},
			MinArgs:       2,
			MaxArgs:       3,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "ABS",
		Type:        ScalarFunction,
		Description: "Returns the absolute value",
		Signature: FunctionSignature{
			ReturnType:    TypeNumeric,
			ArgumentTypes: []DataType{TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(FunctionInfo{
		Name:        "ROUND",
		Type:        ScalarFunction,
		Description: "Rounds a number to a specified precision",
		Signature: FunctionSignature{
			ReturnType:    TypeFloat,
			ArgumentTypes: []DataType{TypeFloat, TypeInteger},
			MinArgs:       1,
			MaxArgs:       2,
		},
	})
}

func TestFunctionValidation(t *testing.T) {
	// Create a new registry and validator
	registry := NewRegistry()
	registerTestFunctions(registry)
	validator := NewValidator(registry)

	// Test cases for function validation
	testCases := []struct {
		name      string
		args      []DataType
		expectErr bool
	}{
		{"COUNT", []DataType{}, false},
		{"COUNT", []DataType{TypeInteger}, false},
		{"COUNT", []DataType{TypeInteger, TypeInteger}, true}, // Too many args

		{"SUM", []DataType{TypeInteger}, false},
		{"SUM", []DataType{TypeFloat}, false},
		{"SUM", []DataType{}, true}, // Too few args

		{"UPPER", []DataType{TypeString}, false},
		{"UPPER", []DataType{TypeInteger}, true}, // Wrong arg type

		{"SUBSTRING", []DataType{TypeString, TypeInteger}, false},                          // Min args
		{"SUBSTRING", []DataType{TypeString, TypeInteger, TypeInteger}, false},             // Max args
		{"SUBSTRING", []DataType{TypeString}, true},                                        // Too few args
		{"SUBSTRING", []DataType{TypeString, TypeInteger, TypeInteger, TypeInteger}, true}, // Too many args
	}

	for _, tc := range testCases {
		var funcInfo *FunctionInfo
		err := validator.ValidateFunctionCall(tc.name, &funcInfo, tc.args)

		if tc.expectErr && err == nil {
			t.Errorf("Expected validation error for function %s with args %v", tc.name, tc.args)
		} else if !tc.expectErr && err != nil {
			t.Errorf("Expected no validation error for function %s with args %v, but got: %v", tc.name, tc.args, err)
		}

		// If validation succeeded, ensure the function info was populated
		if err == nil {
			if funcInfo == nil {
				t.Errorf("Expected function info to be populated for %s", tc.name)
			} else if funcInfo.Name != tc.name {
				t.Errorf("Expected function name to be %s, got %s", tc.name, funcInfo.Name)
			}
		}
	}
}

func TestCustomFunctionRegistration(t *testing.T) {
	// Create a custom registry
	registry := NewRegistry()

	// Register a custom function
	err := registry.Register(FunctionInfo{
		Name:        "CUSTOM_FUNC",
		Type:        ScalarFunction,
		Description: "Custom test function",
		Signature: FunctionSignature{
			ReturnType:    TypeString,
			ArgumentTypes: []DataType{TypeInteger, TypeString},
			MinArgs:       2,
			MaxArgs:       2,
		},
	})

	if err != nil {
		t.Fatalf("Failed to register custom function: %v", err)
	}

	// Verify the function was registered
	info, err := registry.Get("CUSTOM_FUNC")
	if err != nil {
		t.Fatalf("Failed to get custom function: %v", err)
	}

	if info.Name != "CUSTOM_FUNC" {
		t.Errorf("Expected function name CUSTOM_FUNC, got %s", info.Name)
	}

	// Validate the custom function
	validator := NewValidator(registry)

	// Valid arguments
	var funcInfo *FunctionInfo
	err = validator.ValidateFunctionCall("CUSTOM_FUNC", &funcInfo, []DataType{TypeInteger, TypeString})
	if err != nil {
		t.Errorf("Expected validation to pass, but got: %v", err)
	}

	// Invalid arguments (wrong order)
	err = validator.ValidateFunctionCall("CUSTOM_FUNC", &funcInfo, []DataType{TypeString, TypeInteger})
	if err == nil {
		t.Errorf("Expected validation to fail with wrong argument types")
	}
}
