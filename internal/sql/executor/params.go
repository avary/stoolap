package sql

import (
	"database/sql/driver"

	"github.com/semihalev/stoolap/internal/parser"
)

// parameter provides parameter substitution without modifying AST
type parameter []driver.NamedValue

// newParameter creates a new parameter
func newParameter(params []driver.NamedValue) (*parameter, error) {
	if len(params) == 0 {
		return nil, nil
	}

	ps := parameter(params)

	return &ps, nil
}

// GetValue returns the literal value for a parameter
func (ps parameter) GetValue(param *parser.Parameter) driver.NamedValue {
	if len(ps) == 0 {
		return driver.NamedValue{}
	}

	if param.OrderInStatement >= len(ps) {
		return driver.NamedValue{}
	}

	// OrderInStatement is 0-based
	if param.Index == 0 {
		return ps[param.OrderInStatement]
	} else {
		// For named parameters ($N style), use the Index directly
		// Get the substituted value
		for _, nv := range ps {
			if nv.Ordinal == param.Index {
				return nv
			}
		}
	}

	return driver.NamedValue{}
}
