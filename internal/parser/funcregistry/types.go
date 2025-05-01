package funcregistry

import (
	"errors"
	"fmt"
)

// Common errors
var (
	ErrFunctionExists       = errors.New("function already exists")
	ErrFunctionNotFound     = errors.New("function not found")
	ErrInvalidArgumentCount = errors.New("invalid argument count")
	ErrInvalidArgumentType  = errors.New("invalid argument type")
)

// FunctionType represents the type of function
type FunctionType int

const (
	// ScalarFunction represents a scalar function (returns one value for each input row)
	ScalarFunction FunctionType = iota
	// AggregateFunction represents an aggregate function (returns one value for multiple input rows)
	AggregateFunction
	// WindowFunction represents a window function (calculates across rows related to current row)
	WindowFunction
)

// String returns a string representation of the function type
func (ft FunctionType) String() string {
	switch ft {
	case ScalarFunction:
		return "scalar"
	case AggregateFunction:
		return "aggregate"
	case WindowFunction:
		return "window"
	default:
		return "unknown"
	}
}

// DataType represents SQL data types
type DataType int

const (
	// TypeUnknown represents an unknown data type
	TypeUnknown DataType = iota
	// TypeInteger represents integer data type
	TypeInteger
	// TypeFloat represents float data type
	TypeFloat
	// TypeString represents string data type
	TypeString
	// TypeBoolean represents boolean data type
	TypeBoolean
	// TypeDate represents date data type
	TypeDate
	// TypeTime represents time data type
	TypeTime
	// TypeDateTime represents datetime data type
	TypeDateTime
	// TypeArray represents array data type
	TypeArray
	// TypeJSON represents JSON data type
	TypeJSON
	// TypeAny represents any data type (for functions that accept multiple types)
	TypeAny
	// TypeNumeric represents any numeric data type (integer or float)
	TypeNumeric
)

// String returns a string representation of the data type
func (dt DataType) String() string {
	switch dt {
	case TypeInteger:
		return "INTEGER"
	case TypeFloat:
		return "FLOAT"
	case TypeString:
		return "STRING"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeDateTime:
		return "DATETIME"
	case TypeArray:
		return "ARRAY"
	case TypeJSON:
		return "JSON"
	case TypeAny:
		return "ANY"
	case TypeNumeric:
		return "NUMERIC"
	default:
		return "UNKNOWN"
	}
}

// FunctionSignature represents the signature of a function
type FunctionSignature struct {
	// ReturnType is the return type of the function
	ReturnType DataType
	// ArgumentTypes is a list of argument types for the function
	ArgumentTypes []DataType
	// IsVariadic indicates if the last argument can be repeated
	IsVariadic bool
	// MinArgs is the minimum number of arguments
	MinArgs int
	// MaxArgs is the maximum number of arguments (-1 for unlimited)
	MaxArgs int
}

// ValidateArgs validates the argument count and types against the function signature
func (fs *FunctionSignature) ValidateArgs(argTypes []DataType) error {
	// Check argument count
	argCount := len(argTypes)
	if argCount < fs.MinArgs {
		return fmt.Errorf("%w: expected at least %d, got %d",
			ErrInvalidArgumentCount, fs.MinArgs, argCount)
	}

	if fs.MaxArgs >= 0 && argCount > fs.MaxArgs {
		return fmt.Errorf("%w: expected at most %d, got %d",
			ErrInvalidArgumentCount, fs.MaxArgs, argCount)
	}

	// Check argument types
	sigLen := len(fs.ArgumentTypes)
	for i, argType := range argTypes {
		var expectedType DataType

		if i < sigLen {
			expectedType = fs.ArgumentTypes[i]
		} else if fs.IsVariadic && sigLen > 0 {
			// For variadic functions, use the last argument type for additional arguments
			expectedType = fs.ArgumentTypes[sigLen-1]
		} else {
			// This case should not happen with proper argCount validation
			return fmt.Errorf("%w: unexpected argument at position %d",
				ErrInvalidArgumentCount, i+1)
		}

		// TypeAny accepts any argument type
		if expectedType == TypeAny {
			continue
		}

		// TypeNumeric accepts TypeInteger or TypeFloat
		if expectedType == TypeNumeric && (argType == TypeInteger || argType == TypeFloat) {
			continue
		}

		// Exact type match
		if expectedType == argType {
			continue
		}

		// If we got here, the types don't match
		return fmt.Errorf("%w: expected %s for argument %d, got %s",
			ErrInvalidArgumentType, expectedType, i+1, argType)
	}

	return nil
}

// FunctionInfo contains metadata about a registered function
type FunctionInfo struct {
	// Name is the name of the function (case insensitive in SQL)
	Name string
	// Type is the type of the function (scalar, aggregate, window)
	Type FunctionType
	// Signature defines the function's parameter and return types
	Signature FunctionSignature
	// Description provides documentation for the function
	Description string
}
