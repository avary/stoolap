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
package scalar

import (
	"fmt"
	"strconv"
)

// ConvertToFloat64 attempts to convert a value to float64
func ConvertToFloat64(value interface{}) (float64, error) {
	if value == nil {
		return 0, fmt.Errorf("cannot convert nil to float64")
	}

	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to float64: %v", v, err)
		}
		return f, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	}

	return 0, fmt.Errorf("cannot convert %T to float64", value)
}

// ConvertToString converts a value to string
func ConvertToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ConvertToInt64 attempts to convert a value to int64
func ConvertToInt64(value interface{}) (int64, error) {
	if value == nil {
		return 0, fmt.Errorf("cannot convert nil to int64")
	}

	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Try float conversion if integer parsing fails
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, fmt.Errorf("cannot convert string %q to int64: %v", v, err)
			}
			return int64(f), nil
		}
		return i, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	}

	return 0, fmt.Errorf("cannot convert %T to int64", value)
}

// ConvertToBool attempts to convert a value to bool
func ConvertToBool(value interface{}) (bool, error) {
	if value == nil {
		return false, nil
	}

	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		switch v {
		case "1", "true", "TRUE", "True", "T", "t", "yes", "YES", "Yes":
			return true, nil
		case "0", "false", "FALSE", "False", "F", "f", "no", "NO", "No":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert string %q to bool", v)
		}
	}

	return false, fmt.Errorf("cannot convert %T to bool", value)
}
