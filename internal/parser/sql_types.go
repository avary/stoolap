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
