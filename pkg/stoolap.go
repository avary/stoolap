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

package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sync"

	sqlexecutor "github.com/stoolap/stoolap/internal/sql"
	"github.com/stoolap/stoolap/internal/storage"

	// Import the storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// Global engine registry to ensure only one engine instance per DSN
var (
	// engineRegistry maps DSN strings to engine instances
	engineRegistry = make(map[string]*DB)
	// engineMutex protects access to the engine registry
	engineMutex sync.RWMutex
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
// STRICT RULE: Only ONE engine instance can exist per DSN for the entire application lifetime
// Any attempt to open the same DSN again will return the existing engine instance
func Open(dsn string) (*DB, error) {
	// First check if we already have an engine for this DSN in our registry
	engineMutex.RLock()
	db, exists := engineRegistry[dsn]
	engineMutex.RUnlock()

	// If we found an existing engine, return it immediately
	if exists {
		return db, nil
	}

	// Acquire write lock for creating a new engine
	engineMutex.Lock()
	defer engineMutex.Unlock()

	// Double-check after acquiring the lock
	if db, exists := engineRegistry[dsn]; exists {
		return db, nil
	}

	// Not found in registry, create a new engine

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
	engine, err := factory.Create(dsn)
	if err != nil {
		return nil, err
	}

	if err := engine.Open(); err != nil {
		return nil, err
	}

	// Create new DB instance
	db = &DB{
		engine: engine,
	}

	// Add to registry
	engineRegistry[dsn] = db

	return db, nil
}

// Close closes the database connection and releases resources
// This will actually close the engine and remove it from the registry
func (db *DB) Close() error {
	engineMutex.Lock()
	defer engineMutex.Unlock()

	// Find and remove this engine from the registry
	for dsn, registeredDB := range engineRegistry {
		if registeredDB == db {
			delete(engineRegistry, dsn)
			break
		}
	}

	// Actually close the engine
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
