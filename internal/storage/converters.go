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
// Package storage provides storage types and operations
package storage

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

/*
converters.go - Central type conversion utilities

This file contains all conversion functions for handling data types in a consistent way
throughout the codebase. It serves as the single source of truth for:

1. Date/time parsing and formatting with consistent timezone handling
2. Type conversion for storage and query operations
3. Value normalization for consistent comparisons
4. String representation of various data types
5. Time-based range comparison functions

Organization:
- Constants: Format strings and other constants
- Parsing: Functions to parse various string formats into time.Time
- Formatting: Functions to format time.Time into strings
- Normalization: Functions to normalize values for comparison
- Conversion: Functions to convert between different types
- Comparison: Functions to compare values of various types

The functions in this file supersede all other conversion implementations.
Legacy wrappers are maintained for backward compatibility but are marked
as deprecated.
*/

// Standard format constants for consistent handling
const (
	// Date/time formats for consistent use throughout the codebase
	TimestampFormat  = time.RFC3339          // Standard output format for timestamps
	DateFormat       = "2006-01-02"          // Standard output format for dates
	TimeFormat       = "15:04:05.999999999"  // Standard output format for times with nanoseconds
	TimeFormatSimple = "15:04:05"            // Simple time format without nanoseconds
	DateTimeFormat   = "2006-01-02 15:04:05" // Standard date and time without T separator

	// EndOfDayNanos is the nanosecond time for 23:59:59.999999999
	EndOfDayNanos = int64((23*3600+59*60+59)*1e9 + 999999999)
)

// Common formats for parsing various date/time string formats
var (
	// Shared format arrays for all time-related parsing to ensure consistency
	TimestampFormats = []string{
		time.RFC3339,                    // Full precision format
		time.RFC3339,                    // Standard ISO format
		"2006-01-02T15:04:05Z07:00",     // ISO format
		"2006-01-02T15:04:05",           // ISO format without timezone
		"2006-01-02T15:04:05Z",          // ISO format without timezone with Z
		"2006-01-02 15:04:05.999999999", // SQL-style with high precision
		"2006-01-02 15:04:05.999",       // SQL-style with fractional seconds
		"2006-01-02 15:04:05",           // SQL-style without fractional seconds
		"2006-01-02",                    // Date only (will set time to 00:00:00)
		"2006/01/02 15:04:05",           // Alternative format with slashes
		"2006/01/02",                    // Alternative date only
		"2006-01-02",                    // ISO date format
		"2006/01/02",                    // Alternative date format
		"01/02/2006",                    // US format
		"02/01/2006",                    // European format
		"Jan 02, 2006",                  // Month name format
		"January 02, 2006",              // Full month name format
		"15:04:05.999999999",            // High precision format
		"15:04:05.999",                  // Millisecond format
		"15:04:05",                      // Standard time format
		"15:04",                         // Hours and minutes only
		"3:04:05 PM",                    // 12-hour format
		"3:04 PM",                       // 12-hour format without seconds
	}
)

// ParseTimestamp parses a timestamp string with extensive format support
func ParseTimestamp(value string) (time.Time, error) {
	// Try all registered timestamp formats
	for _, format := range TimestampFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			return t.UTC(), nil // Normalize to UTC for consistency
		}
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", value)
}

// FormatTimestamp formats a timestamp consistently
func FormatTimestamp(t time.Time) string {
	return t.Format(time.RFC3339)
}

// ConvertValueToString converts a value to its string representation for indexing
// Deprecated: Use ValueToString instead
func ConvertValueToString(value interface{}) string {
	return ValueToString(value)
}

// ValueToString converts any value to a string consistently
func ValueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		return FormatTimestamp(v)
	case ColumnValue:
		// Handle ColumnValue interface
		if v == nil || v.IsNull() {
			return ""
		}

		switch v.Type() {
		case INTEGER:
			if i, ok := v.AsInt64(); ok {
				return strconv.FormatInt(i, 10)
			}
		case FLOAT:
			if f, ok := v.AsFloat64(); ok {
				return strconv.FormatFloat(f, 'f', -1, 64)
			}
		case TEXT, JSON:
			if s, ok := v.AsString(); ok {
				return s
			}
		case BOOLEAN:
			if b, ok := v.AsBoolean(); ok {
				if b {
					return "true"
				}
				return "false"
			}
		case TIMESTAMP:
			if t, ok := v.AsTimestamp(); ok {
				return FormatTimestamp(t)
			}
		}

		// Fallback to string representation
		if s, ok := v.AsString(); ok {
			return s
		}
	default:
		// For complex types, try JSON marshaling
		data, err := json.Marshal(v)
		if err == nil {
			return string(data)
		}
		// Last resort
		return fmt.Sprintf("%v", v)
	}

	return ""
}

// ConvertToColumnValue converts an interface{} value to a ColumnValue
// based on the provided data type
// Deprecated: Use ValueToColumnValue instead, which has more complete support
func ConvertToColumnValue(value interface{}, dataType DataType) ColumnValue {
	return ValueToColumnValue(value, dataType)
}

// GetColumnType returns the data type of a column by name from the schema
func GetColumnType(schema Schema, columnName string) DataType {
	for _, col := range schema.Columns {
		if col.Name == columnName {
			return col.Type
		}
	}
	return NULL
}

// ValueToPooledColumnValue converts a value to a ColumnValue using a pool
func ValueToPooledColumnValue(value interface{}, dataType DataType) ColumnValue {
	if value == nil {
		return NewNullValue(dataType)
	}

	switch dataType {
	case INTEGER:
		switch v := value.(type) {
		case int:
			return GetPooledIntegerValue(int64(v))
		case int64:
			return GetPooledIntegerValue(v)
		case float64:
			return GetPooledIntegerValue(int64(v))
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return GetPooledIntegerValue(i)
			}
		}
		return NewNullIntegerValue()

	case FLOAT:
		switch v := value.(type) {
		case float64:
			return GetPooledFloatValue(v)
		case int:
			return GetPooledFloatValue(float64(v))
		case int64:
			return GetPooledFloatValue(float64(v))
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return GetPooledFloatValue(f)
			}
		}
		return NewNullFloatValue()

	case TEXT:
		switch v := value.(type) {
		case string:
			return GetPooledStringValue(v)
		case []byte:
			return GetPooledStringValue(string(v))
		default:
			return GetPooledStringValue(ValueToString(value))
		}

	case BOOLEAN:
		switch v := value.(type) {
		case bool:
			return NewBooleanValue(v)
		case int:
			return NewBooleanValue(v != 0)
		case int64:
			return NewBooleanValue(v != 0)
		case string:
			switch v {
			case "true", "1", "t", "yes", "y":
				return NewBooleanValue(true)
			case "false", "0", "f", "no", "n":
				return NewBooleanValue(false)
			}
		}
		return NewNullBooleanValue()

	case TIMESTAMP:
		switch v := value.(type) {
		case time.Time:
			return GetPooledTimestampValue(v)
		case string:
			if t, err := ParseTimestamp(v); err == nil {
				return GetPooledTimestampValue(t)
			}
		}
		return NewNullTimestampValue()

	case JSON:
		switch v := value.(type) {
		case string:
			// Validate JSON
			var jsonData interface{}
			if err := json.Unmarshal([]byte(v), &jsonData); err == nil {
				return GetPooledJSONValue(v)
			}
		case []byte:
			var jsonData interface{}
			if err := json.Unmarshal(v, &jsonData); err == nil {
				return GetPooledJSONValue(string(v))
			}
		case map[string]interface{}, []interface{}:
			if jsonBytes, err := json.Marshal(v); err == nil {
				return GetPooledJSONValue(string(jsonBytes))
			}
		}
		return NewNullJSONValue()
	}

	return NewNullValue(dataType)
}

// ValueToColumnValue consistently converts any Go value to a ColumnValue
// based on the data type
func ValueToColumnValue(value interface{}, dataType DataType) ColumnValue {
	if value == nil {
		return NewNullValue(dataType)
	}

	switch dataType {
	case INTEGER:
		switch v := value.(type) {
		case int:
			return NewIntegerValue(int64(v))
		case int64:
			return NewIntegerValue(v)
		case float64:
			return NewIntegerValue(int64(v))
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return NewIntegerValue(i)
			}
		}
		return NewNullIntegerValue()

	case FLOAT:
		switch v := value.(type) {
		case float64:
			return NewFloatValue(v)
		case int:
			return NewFloatValue(float64(v))
		case int64:
			return NewFloatValue(float64(v))
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return NewFloatValue(f)
			}
		}
		return NewNullFloatValue()

	case TEXT:
		switch v := value.(type) {
		case string:
			return NewStringValue(v)
		case []byte:
			return NewStringValue(string(v))
		default:
			return NewStringValue(ValueToString(value))
		}

	case BOOLEAN:
		switch v := value.(type) {
		case bool:
			return NewBooleanValue(v)
		case int:
			return NewBooleanValue(v != 0)
		case int64:
			return NewBooleanValue(v != 0)
		case string:
			switch v {
			case "true", "1", "t", "yes", "y":
				return NewBooleanValue(true)
			case "false", "0", "f", "no", "n":
				return NewBooleanValue(false)
			}
		}
		return NewNullBooleanValue()

	case TIMESTAMP:
		switch v := value.(type) {
		case time.Time:
			return NewTimestampValue(v)
		case string:
			if t, err := ParseTimestamp(v); err == nil {
				return NewTimestampValue(t)
			}
		}
		return NewNullTimestampValue()

	case JSON:
		switch v := value.(type) {
		case string:
			// Validate JSON
			var jsonData interface{}
			if err := json.Unmarshal([]byte(v), &jsonData); err == nil {
				return NewJSONValue(v)
			}
		case []byte:
			var jsonData interface{}
			if err := json.Unmarshal(v, &jsonData); err == nil {
				return NewJSONValue(string(v))
			}
		case map[string]interface{}, []interface{}:
			if jsonBytes, err := json.Marshal(v); err == nil {
				return NewJSONValue(string(jsonBytes))
			}
		}
		return NewNullJSONValue()
	}

	return NewNullValue(dataType)
}

// ValueToTypedValue converts a value to a specific data type
func ValueToTypedValue(value interface{}, dataType DataType) interface{} {
	if value == nil {
		return nil
	}

	switch dataType {
	case INTEGER:
		switch v := value.(type) {
		case int:
			return int64(v)
		case int64:
			return value
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil

	case FLOAT:
		switch v := value.(type) {
		case float64:
			return value
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil

	case TEXT:
		switch v := value.(type) {
		case string:
			return value
		case []byte:
			return string(v)
		default:
			return ValueToString(value)
		}

	case BOOLEAN:
		switch v := value.(type) {
		case bool:
			return value
		case int:
			return v != 0
		case int64:
			return v != 0
		case string:
			switch v {
			case "true", "1", "t", "yes", "y":
				return true
			case "false", "0", "f", "no", "n":
				return false
			}
		}
		return nil

	case TIMESTAMP:
		switch v := value.(type) {
		case time.Time:
			return value
		case string:
			if t, err := ParseTimestamp(v); err == nil {
				return t
			}
		}
		return nil

	case JSON:
		switch v := value.(type) {
		case string:
			// Validate JSON
			var jsonData interface{}
			if err := json.Unmarshal([]byte(v), &jsonData); err == nil {
				return value
			}
		case []byte:
			var jsonData interface{}
			if err := json.Unmarshal(v, &jsonData); err == nil {
				return string(v)
			}
		case map[string]interface{}, []interface{}:
			if jsonBytes, err := json.Marshal(v); err == nil {
				return string(jsonBytes)
			}
		}
		return nil
	}

	return nil
}

// ConvertGoValueToStorageValue normalizes Go values to consistent storage types
// Deprecated: Consider using ValueToColumnValue for more robust type handling
func ConvertGoValueToStorageValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32, uint64:
		// Convert all integer types to int64
		return ValueToColumnValue(v, INTEGER).AsInterface()
	case float32:
		return float64(v)
	case []byte:
		return string(v)
	default:
		return v
	}
}

// ConvertStorageValueToGoValue converts a storage value to a Go value
// Deprecated: Use ColumnValue methods directly when possible
func ConvertStorageValueToGoValue(value interface{}, destType DataType) interface{} {
	// If we already have a ColumnValue, use it directly
	if cv, ok := value.(ColumnValue); ok {
		return cv.AsInterface()
	}

	// Handle special JSON case to preserve maps and arrays
	if destType == JSON {
		// For maps and arrays, pass through as-is
		switch v := value.(type) {
		case map[string]interface{}, []interface{}:
			return v
		case string:
			// For JSON strings, just return as-is
			return v
		}
	}

	// Otherwise create a temporary column value and extract interface value
	return ValueToColumnValue(value, destType).AsInterface()
}

// CompareTimestamps compares two timestamps with nanosecond precision
// Returns: -1 if ts1 < ts2, 0 if equal, 1 if ts1 > ts2
func CompareTimestamps(ts1, ts2 time.Time) int {
	// Ensure both timestamps are in UTC
	ts1 = ts1.UTC()
	ts2 = ts2.UTC()

	// Compare using time.Time.Compare()
	return ts1.Compare(ts2)
}

// NormalizeForDataType normalizes a time.Time value based on the data type
// This ensures consistent handling across the codebase
func NormalizeForDataType(t time.Time, dataType DataType) time.Time {
	switch dataType {
	case TIMESTAMP:
		return t.UTC()
	default:
		return t.UTC()
	}
}

// CompareTimeValues compares two time values based on their data type
// This is a generic comparison function that handles DATE, TIME, and TIMESTAMP types
// Returns: -1 if t1 < t2, 0 if equal, 1 if t1 > t2
func CompareTimeValues(t1, t2 time.Time, dataType DataType) int {
	switch dataType {
	case TIMESTAMP:
		return CompareTimestamps(t1, t2)
	default:
		return t1.Compare(t2)
	}
}

// ScanColumnValueToDestination scans a ColumnValue into a destination pointer
// This is a central utility function for converting ColumnValues to Go types in scan operations
// Based on the high-performance scan implementations from mvcc scan_utils.go
func ScanColumnValueToDestination(value ColumnValue, dest interface{}) error {
	// Handle nil values first
	if value == nil || value.IsNull() {
		return scanNullValue(dest)
	}

	// Try direct path for DirectValue with valueRef (most efficient path)
	if dv, ok := value.(*DirectValue); ok && dv.AsInterface() != nil {
		// Check common destination types for direct assignments
		switch d := dest.(type) {
		case *interface{}:
			// For interface{} destinations, use the reference directly
			*d = dv.AsInterface()
			return nil
		case *string:
			if strVal, ok := dv.AsInterface().(string); ok {
				*d = strVal
				return nil
			}
		case *int:
			if intVal, ok := dv.AsInterface().(int); ok {
				*d = intVal
				return nil
			}
			if i64Val, ok := dv.AsInterface().(int64); ok {
				*d = int(i64Val)
				return nil
			}
		case *int64:
			if i64Val, ok := dv.AsInterface().(int64); ok {
				*d = i64Val
				return nil
			}
			if intVal, ok := dv.AsInterface().(int); ok {
				*d = int64(intVal)
				return nil
			}
		case *float64:
			if floatVal, ok := dv.AsInterface().(float64); ok {
				*d = floatVal
				return nil
			}
		case *bool:
			if boolVal, ok := dv.AsInterface().(bool); ok {
				*d = boolVal
				return nil
			}
		case *time.Time:
			if timeVal, ok := dv.AsInterface().(time.Time); ok {
				*d = timeVal
				return nil
			}
		}
	}

	// Try direct path for typed destinations using reflection-free approach
	ptrType := reflect.TypeOf(dest)
	if ptrType != nil && ptrType.Kind() == reflect.Ptr {
		// Special fast paths for common types
		switch ptrType.Elem().Kind() {
		case reflect.Int, reflect.Int64:
			if i, ok := value.AsInt64(); ok {
				reflect.ValueOf(dest).Elem().SetInt(i)
				return nil
			}
			if f, ok := value.AsFloat64(); ok {
				reflect.ValueOf(dest).Elem().SetInt(int64(f))
				return nil
			}
			return fmt.Errorf("cannot convert %v to int/int64", value.Type())

		case reflect.Float64:
			if f, ok := value.AsFloat64(); ok {
				reflect.ValueOf(dest).Elem().SetFloat(f)
				return nil
			}
			if i, ok := value.AsInt64(); ok {
				reflect.ValueOf(dest).Elem().SetFloat(float64(i))
				return nil
			}
			return fmt.Errorf("cannot convert %v to float64", value.Type())

		case reflect.Bool:
			if b, ok := value.AsBoolean(); ok {
				reflect.ValueOf(dest).Elem().SetBool(b)
				return nil
			}
			return fmt.Errorf("cannot convert %v to bool", value.Type())

		case reflect.String:
			if s, ok := value.AsString(); ok {
				reflect.ValueOf(dest).Elem().SetString(s)
				return nil
			}
			// Try to convert other types to string
			s := ValueToString(value)
			reflect.ValueOf(dest).Elem().SetString(s)
			return nil

		case reflect.Struct:
			// Handle time.Time specially
			if ptrType.Elem() == reflect.TypeOf(time.Time{}) {
				if t, ok := value.AsTimestamp(); ok {
					reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(t))
					return nil
				}
				return fmt.Errorf("cannot convert %v to time.Time", value.Type())
			}
		}
	}

	// Handle interface{} destination specially
	if d, ok := dest.(*interface{}); ok {
		*d = value.AsInterface()
		return nil
	}

	// Fallback to type switch-based approach for remaining cases
	switch d := dest.(type) {
	case *string:
		// For string destinations, use AsString with fallback to string representation
		if s, ok := value.AsString(); ok {
			*d = s
			return nil
		}
		*d = ValueToString(value)
		return nil

	case *int:
		// For int destinations, try integer then float conversion
		if i, ok := value.AsInt64(); ok {
			*d = int(i)
			return nil
		}
		if f, ok := value.AsFloat64(); ok {
			*d = int(f)
			return nil
		}
		return fmt.Errorf("cannot convert %v to int", value.Type())

	case *int64:
		// For int64 destinations, try integer then float conversion
		if i, ok := value.AsInt64(); ok {
			*d = i
			return nil
		}
		if f, ok := value.AsFloat64(); ok {
			*d = int64(f)
			return nil
		}
		return fmt.Errorf("cannot convert %v to int64", value.Type())

	case *float64:
		// For float64 destinations, try float then integer conversion
		if f, ok := value.AsFloat64(); ok {
			*d = f
			return nil
		}
		if i, ok := value.AsInt64(); ok {
			*d = float64(i)
			return nil
		}
		return fmt.Errorf("cannot convert %v to float64", value.Type())

	case *bool:
		// For boolean destinations, use AsBoolean
		if b, ok := value.AsBoolean(); ok {
			*d = b
			return nil
		}
		return fmt.Errorf("cannot convert %v to bool", value.Type())

	case *time.Time:
		// For time.Time destinations, try timestamp, date, and time in that order
		if t, ok := value.AsTimestamp(); ok {
			*d = t
			return nil
		}
		return fmt.Errorf("cannot convert %v to time.Time", value.Type())

	default:
		return fmt.Errorf("unsupported destination type: %T", dest)
	}
}

// scanNullValue handles setting null/zero values for destination pointers
func scanNullValue(dest interface{}) error {
	switch d := dest.(type) {
	case *interface{}:
		*d = nil
	case *string:
		*d = ""
	case *int:
		*d = 0
	case *int64:
		*d = 0
	case *float64:
		*d = 0
	case *bool:
		*d = false
	case *time.Time:
		*d = time.Time{}
	default:
		// Try using reflection for other types
		ptrVal := reflect.ValueOf(dest)
		if ptrVal.Kind() == reflect.Ptr && ptrVal.Elem().CanSet() {
			// Set zero value of the appropriate type
			ptrVal.Elem().Set(reflect.Zero(ptrVal.Elem().Type()))
			return nil
		}
		return fmt.Errorf("unsupported destination type for NULL value: %T", dest)
	}
	return nil
}

// IsTimeInRange checks if a time value is within a given range based on data type
// This consolidates all the range checking logic for dates, times, and timestamps
func IsTimeInRange(target, min, max time.Time,
	dataType DataType,
	minInclusive, maxInclusive bool) bool {

	// Normalize values based on data type and inclusivity
	target = NormalizeForDataType(target, dataType)

	// Handle minimum boundary
	if !min.IsZero() {
		min = NormalizeForDataType(min, dataType)

		// For non-inclusive boundary (>), it must be strictly greater
		if !minInclusive {
			if CompareTimeValues(target, min, dataType) <= 0 {
				return false
			}
		} else {
			// For inclusive boundary (>=), it must be greater or equal
			if CompareTimeValues(target, min, dataType) < 0 {
				return false
			}
		}
	}

	// Handle maximum boundary
	if !max.IsZero() {
		max = NormalizeForDataType(max, dataType)

		// For non-inclusive boundary (<), it must be strictly less
		if !maxInclusive {
			if CompareTimeValues(target, max, dataType) >= 0 {
				return false
			}
		} else {
			// For inclusive boundary (<=), it must be less or equal
			if CompareTimeValues(target, max, dataType) > 0 {
				return false
			}
		}
	}

	return true
}
