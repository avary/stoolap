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
	TimestampFormat  = time.RFC3339Nano      // Standard output format for timestamps
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
		time.RFC3339Nano,                // Full precision format
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
	}

	// Date formats to try
	DateFormats = []string{
		"2006-01-02",       // ISO date format
		"2006/01/02",       // Alternative date format
		"01/02/2006",       // US format
		"02/01/2006",       // European format
		"Jan 02, 2006",     // Month name format
		"January 02, 2006", // Full month name format
	}

	// Time formats to try
	TimeFormats = []string{
		"15:04:05.999999999", // High precision format
		"15:04:05.999",       // Millisecond format
		"15:04:05",           // Standard time format
		"15:04",              // Hours and minutes only
		"3:04:05 PM",         // 12-hour format
		"3:04 PM",            // 12-hour format without seconds
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

	// If we couldn't parse it as a timestamp, try as a date
	t, err := ParseDate(value)
	if err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", value)
}

// ParseTime parses a time string with better format support
func ParseTime(value string) (time.Time, error) {
	// First try all registered time formats
	for _, format := range TimeFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			// Always normalize to year 1 for time-only values
			return NormalizeTime(t), nil
		}
	}

	// Also try as timestamp and extract time portion
	t, err := ParseTimestamp(value)
	if err == nil {
		return NormalizeTime(t), nil
	}

	return time.Time{}, fmt.Errorf("invalid time format: %s", value)
}

// ParseDate parses a date string with better format support
func ParseDate(value string) (time.Time, error) {
	// First try all registered date formats
	for _, format := range DateFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			// Always normalize to start of day in UTC
			return NormalizeDate(t), nil
		}
	}

	// Try timestamp formats and extract date portion
	for _, format := range TimestampFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			// Normalize to start of day in UTC
			return NormalizeDate(t), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format: %s", value)
}

// NormalizeDate ensures a time value only contains date information (time set to 00:00:00)
func NormalizeDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// NormalizeTime ensures a time value only contains time information (date set to 0001-01-01)
func NormalizeTime(t time.Time) time.Time {
	return time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// DateToEndOfDay converts a date to the last nanosecond of that day (23:59:59.999999999)
func DateToEndOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, time.UTC)
}

// FormatTimestamp formats a timestamp consistently
func FormatTimestamp(t time.Time) string {
	// For dates (time is midnight), use date format
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
		return t.Format(DateFormat)
	}
	return t.Format(TimestampFormat)
}

// FormatDate formats a date consistently (without time component)
func FormatDate(t time.Time) string {
	return t.Format(DateFormat)
}

// FormatTime formats a time consistently (without date component)
func FormatTime(t time.Time) string {
	if t.Nanosecond() == 0 {
		return t.Format(TimeFormatSimple)
	}
	return t.Format(TimeFormat)
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
		// Use date-only format for values that appear to be just dates
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return FormatDate(v)
		}
		// For times with no date component (year 1)
		if v.Year() == 1 && v.Month() == 1 && v.Day() == 1 {
			return FormatTime(v)
		}
		// Regular timestamp
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
		case DATE:
			if d, ok := v.AsDate(); ok {
				return FormatDate(d)
			}
		case TIME:
			if t, ok := v.AsTime(); ok {
				return FormatTime(t)
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

// NormalizeDateForRange normalizes a date value for range comparisons
// For max boundaries with inclusive flag, it sets the time to end of day
func NormalizeDateForRange(date time.Time, isMaxBoundary, inclusive bool) time.Time {
	// For inclusive max boundary (<=), set to end of day to include the full day
	if isMaxBoundary && inclusive {
		return DateToEndOfDay(date)
	}

	// Otherwise, normalize to start of day
	return NormalizeDate(date)
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

	case DATE:
		switch v := value.(type) {
		case time.Time:
			return NewDateValue(NormalizeDate(v))
		case string:
			if t, err := ParseDate(v); err == nil {
				return NewDateValue(t)
			}
		}
		return NewNullDateValue()

	case TIME:
		switch v := value.(type) {
		case time.Time:
			return NewTimeValue(NormalizeTime(v))
		case string:
			if t, err := ParseTime(v); err == nil {
				return NewTimeValue(t)
			}
		}
		return NewNullTimeValue()

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

// CompareDates compares two dates with proper normalization
// Returns: -1 if date1 < date2, 0 if equal, 1 if date1 > date2
func CompareDates(date1, date2 time.Time) int {
	// Normalize both dates to start of day in UTC
	date1 = NormalizeDate(date1)
	date2 = NormalizeDate(date2)

	// Compare using time.Time.Compare()
	return date1.Compare(date2)
}

// CompareTimes compares two time values with proper normalization
// This ensures consistent comparison of time-only values regardless of date component
// Returns: -1 if time1 < time2, 0 if equal, 1 if time1 > time2
func CompareTimes(time1, time2 time.Time) int {
	// Normalize both times to same date in UTC
	time1 = NormalizeTime(time1)
	time2 = NormalizeTime(time2)

	// Compare using time.Time.Compare()
	return time1.Compare(time2)
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
	case DATE:
		return NormalizeDate(t)
	case TIME:
		return NormalizeTime(t)
	case TIMESTAMP:
		return t.UTC()
	default:
		return t
	}
}

// CompareTimeValues compares two time values based on their data type
// This is a generic comparison function that handles DATE, TIME, and TIMESTAMP types
// Returns: -1 if t1 < t2, 0 if equal, 1 if t1 > t2
func CompareTimeValues(t1, t2 time.Time, dataType DataType) int {
	switch dataType {
	case DATE:
		return CompareDates(t1, t2)
	case TIME:
		return CompareTimes(t1, t2)
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
				if d, ok := value.AsDate(); ok {
					reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(d))
					return nil
				}
				if t, ok := value.AsTime(); ok {
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
		if date, ok := value.AsDate(); ok {
			*d = date
			return nil
		}
		if t, ok := value.AsTime(); ok {
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
		// For DATE types with inclusive max, set to end of day
		if dataType == DATE && maxInclusive {
			max = DateToEndOfDay(max)
		} else {
			max = NormalizeForDataType(max, dataType)
		}

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
