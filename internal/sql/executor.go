// Package sql provides SQL execution functionality
package sql

import (
	"context"
	"database/sql/driver"

	sql "github.com/stoolap/stoolap/internal/sql/executor"
	"github.com/stoolap/stoolap/internal/storage"
)

// NewExecutor creates a new SQL executor
func NewExecutor(engine storage.Engine) *Executor {
	// Create the real executor
	sqlExecutor := sql.NewExecutor(engine)

	return &Executor{
		sqlExecutor: sqlExecutor,
	}
}

// Executor executes SQL statements
type Executor struct {
	sqlExecutor *sql.Executor
}

// Execute executes a SQL statement
func (e *Executor) Execute(ctx context.Context, tx storage.Transaction, query string) (storage.Result, error) {
	return e.sqlExecutor.ExecuteWithParams(ctx, tx, query, nil)
}

// ExecuteWithParams executes a SQL statement with parameters
func (e *Executor) ExecuteWithParams(ctx context.Context, tx storage.Transaction, query string, params []driver.NamedValue) (storage.Result, error) {
	return e.sqlExecutor.ExecuteWithParams(ctx, tx, query, params)
}

// EnableVectorizedMode enables vectorized execution for appropriate query types
func (e *Executor) EnableVectorizedMode() {
	e.sqlExecutor.EnableVectorizedMode()
}

// DisableVectorizedMode disables vectorized execution
func (e *Executor) DisableVectorizedMode() {
	e.sqlExecutor.DisableVectorizedMode()
}

// IsVectorizedModeEnabled returns whether vectorized execution is enabled
func (e *Executor) IsVectorizedModeEnabled() bool {
	return e.sqlExecutor.IsVectorizedModeEnabled()
}
