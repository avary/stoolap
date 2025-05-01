package storage

// Condition represents a WHERE condition for SQL queries
type Condition struct {
	ColumnName string      // Name of the column (original name, not alias)
	Operator   Operator    // Comparison operator
	Value      interface{} // Value to compare against

	// For complex expressions pushed down to storage
	Expression Expression // If not nil, this is a complex expression to use instead of the simple condition

	// Internal fields for alias handling
	originalName string            // Original column name if ColumnName is an alias
	aliases      map[string]string // Map of column aliases (alias -> original)
}

// NewCondition creates a new condition
func NewCondition(columnName string, operator Operator, value interface{}) *Condition {
	return &Condition{
		ColumnName: columnName,
		Operator:   operator,
		Value:      value,
	}
}

// WithAliases sets the column aliases for the condition
// If the column name is an alias, it will be resolved to the original column name
func (c *Condition) WithAliases(aliases map[string]string) *Condition {
	c.aliases = aliases

	// Check if the column name is an alias and resolve it if needed
	if originalName, isAlias := aliases[c.ColumnName]; isAlias {
		c.originalName = c.ColumnName // Keep track of the original alias
		c.ColumnName = originalName   // Replace with the actual column name
	}

	return c
}

// GetOriginalName returns the original column name (alias) if this condition
// was created with an alias that was resolved to a real column name
func (c *Condition) GetOriginalName() string {
	return c.originalName
}

// GetAliases returns the map of aliases used by this condition
func (c *Condition) GetAliases() map[string]string {
	return c.aliases
}
