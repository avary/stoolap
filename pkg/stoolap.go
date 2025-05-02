package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"

	sqlexecutor "github.com/stoolap/stoolap/internal/sql"
	"github.com/stoolap/stoolap/internal/storage"

	// Import the storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// DB represents a stoolap database
type DB struct {
	engine storage.Engine
}

// Storage engine constants
const (
	// MemoryScheme is the in-memory storage engine URI scheme
	MemoryScheme = "memory"
	// FileScheme is the persistent file storage engine URI scheme
	FileScheme = "file"
)

// Open opens a database connection
func Open(dsn string) (*DB, error) {
	var engine storage.Engine

	// Parse URL to validate and extract scheme
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string format: %w", err)
	}

	// Validate scheme
	switch parsedURL.Scheme {
	case MemoryScheme:
		// Memory scheme is valid
	case FileScheme:
		// File scheme is valid - ensure path exists
		// We need to check for both host and path - either one or both must have content
		if (parsedURL.Path == "" || parsedURL.Path == "/") && parsedURL.Host == "" {
			return nil, errors.New("file:// scheme requires a non-empty path")
		}
	default:
		return nil, errors.New("unsupported connection string format: use 'memory://' for in-memory or 'file://path' for persistent storage")
	}

	// Use the storage engine factory to create the engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		return nil, errors.New("database storage engine factory not found")
	}

	// Create the engine with the validated connection string
	engine, err = factory.Create(dsn)
	if err != nil {
		return nil, err
	}

	if err := engine.Open(); err != nil {
		return nil, err
	}

	return &DB{
		engine: engine,
	}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.engine.Close()
}

// Engine returns the underlying storage engine
func (db *DB) Engine() storage.Engine {
	return db.engine
}

// Executor represents a SQL query executor
type Executor interface {
	Execute(ctx context.Context, tx storage.Transaction, query string) (storage.Result, error)
	ExecuteWithParams(ctx context.Context, tx storage.Transaction, query string, params []driver.NamedValue) (storage.Result, error)
	EnableVectorizedMode()
	DisableVectorizedMode()
	IsVectorizedModeEnabled() bool
}

// Executor returns a new SQL executor for the database
func (db *DB) Executor() Executor {
	return sqlexecutor.NewExecutor(db.engine)
}

// Exec executes a query without returning any rows
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	// Execute the query with the provided context
	tx, err := db.engine.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	// Create an executor
	executor := sqlexecutor.NewExecutor(db.engine)

	// Execute the query
	result, err := executor.Execute(ctx, tx, query)
	if err != nil {
		// Explicitly rollback the transaction on error
		tx.Rollback()
		return nil, err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// Check if we got a Result or not
	if result != nil {
		defer result.Close()

		// Create and return result
		return &execResult{
			rowsAffected: result.RowsAffected(),
			lastInsertID: result.LastInsertID(),
		}, nil
	}

	// No result, return empty result
	return &execResult{}, nil
}

// execResult implements sql.Result
type execResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Query executes a query and returns rows
func (db *DB) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return nil, errors.New("direct Query not supported, use database/sql with stoolap driver")
}
