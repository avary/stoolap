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
	"fmt"
	"strings"
	"sync"
)

// Registry is the interface for a function registry
type Registry interface {
	// Register registers a new function
	Register(info FunctionInfo) error

	// Get retrieves a function by name
	Get(name string) (FunctionInfo, error)

	// List returns all registered functions, optionally filtered by type
	List(functionType FunctionType) []FunctionInfo

	// ValidateCall validates a function call with the given name and argument types
	ValidateCall(name string, argTypes []DataType) error

	// MustRegister registers a function and panics if registration fails
	MustRegister(info FunctionInfo)
}

// DefaultRegistry implements the Registry interface
type DefaultRegistry struct {
	mu        sync.RWMutex
	functions map[string]FunctionInfo
}

// NewRegistry creates a new function registry
func NewRegistry() Registry {
	registry := &DefaultRegistry{
		functions: make(map[string]FunctionInfo),
	}

	return registry
}

// Register adds a function to the registry
func (r *DefaultRegistry) Register(info FunctionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Function names are case-insensitive in SQL
	name := strings.ToUpper(info.Name)

	// Check if function already exists
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("%w: %s", ErrFunctionExists, info.Name)
	}

	// Store the function info with uppercase name
	r.functions[name] = info

	return nil
}

// Get retrieves a function from the registry by name
func (r *DefaultRegistry) Get(name string) (FunctionInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	info, exists := r.functions[name]
	if !exists {
		return FunctionInfo{}, fmt.Errorf("%w: %s", ErrFunctionNotFound, name)
	}

	return info, nil
}

// List returns all registered functions, optionally filtered by type
func (r *DefaultRegistry) List(functionType FunctionType) []FunctionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []FunctionInfo

	for _, info := range r.functions {
		// If functionType is negative, return all functions
		if functionType < 0 || info.Type == functionType {
			result = append(result, info)
		}
	}

	return result
}

// ValidateCall validates a function call with the given name and argument types
func (r *DefaultRegistry) ValidateCall(name string, argTypes []DataType) error {
	info, err := r.Get(name)
	if err != nil {
		return err
	}

	return info.Signature.ValidateArgs(argTypes)
}

// MustRegister registers a function and panics if registration fails
func (r *DefaultRegistry) MustRegister(info FunctionInfo) {
	if err := r.Register(info); err != nil {
		panic(fmt.Sprintf("Failed to register function %s: %v", info.Name, err))
	}
}
