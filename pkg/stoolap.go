package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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

// StorageType represents the type of storage engine to use
type StorageType string

const (
	// StorageTypeDB is the database storage engine
	StorageTypeDB StorageType = "db"
)

// Open opens a database connection
func Open(dsn string) (*DB, error) {
	var engine storage.Engine

	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	storageType := StorageTypeDB
	if uri.Scheme != "" {
		storageType = StorageType(uri.Scheme)
	}

	// Only support db:// connection string
	if storageType != StorageTypeDB {
		return nil, errors.New("unsupported storage type: " + string(storageType) + " (only db:// is supported)")
	}

	path := uri.Path
	if path == "" {
		path = "/stoolap.db"
	}

	// Create connection string for the engine
	connString := "db://" + path

	// Add any query parameters
	if uri.RawQuery != "" {
		connString += "?" + uri.RawQuery
	}

	// Use the storage engine factory to create the engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		return nil, errors.New("database storage engine factory not found")
	}

	engine, err = factory.Create(connString)
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
