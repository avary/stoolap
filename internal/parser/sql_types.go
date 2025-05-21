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

package parser

// SqlType represents a SQL data type
type SqlType int

const (
	// Invalid represents an invalid data type
	Invalid SqlType = iota
	// Integer represents an INTEGER data type
	Integer
	// Float represents a FLOAT data type
	Float
	// Text represents a TEXT data type
	Text
	// Boolean represents a BOOLEAN data type
	Boolean
	// Timestamp represents a TIMESTAMP data type
	Timestamp
	// Date represents a DATE data type
	Date
	// Time represents a TIME data type
	Time
	// Json represents a JSON data type
	Json
)

// String returns a string representation of the SQL type
func (s SqlType) String() string {
	switch s {
	case Integer:
		return "INTEGER"
	case Float:
		return "FLOAT"
	case Text:
		return "TEXT"
	case Boolean:
		return "BOOLEAN"
	case Timestamp:
		return "TIMESTAMP"
	case Date:
		return "DATE"
	case Time:
		return "TIME"
	case Json:
		return "JSON"
	default:
		return "INVALID"
	}
}
