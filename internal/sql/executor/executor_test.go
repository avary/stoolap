package sql

import (
	"context"
	"testing"

	"github.com/stoolap/stoolap/internal/storage"
)

// This is a minimal test to verify that the executor can be created
// and basic methods can be called without panicking.
func TestExecutorCreation(t *testing.T) {
	// Create a mock engine
	mockEngine := &mockEngine{}

	// Create an executor
	executor := NewExecutor(mockEngine)
	if executor == nil {
		t.Fatalf("Failed to create executor")
	}

	// Check that the executor has a function registry
	if executor.functionRegistry == nil {
		t.Errorf("Executor has nil function registry")
	}
}

// mockEngine is a simple mock implementation of storage.Engine
type mockEngine struct{}

func (m *mockEngine) Path() string {
	return "/mock/path"
}

func (m *mockEngine) Open() error {
	return nil
}

func (m *mockEngine) Close() error {
	return nil
}

func (m *mockEngine) BeginTx(ctx context.Context) (storage.Transaction, error) {
	return &mockTransaction{}, nil
}

func (m *mockEngine) BeginTransaction() (storage.Transaction, error) {
	return &mockTransaction{}, nil
}

func (m *mockEngine) TableExists(name string) (bool, error) {
	return false, nil
}

func (m *mockEngine) GetTableSchema(name string) (storage.Schema, error) {
	return storage.Schema{}, nil
}

func (m *mockEngine) ListTables() ([]string, error) {
	return nil, nil
}

func (m *mockEngine) GetConfig() storage.Config {
	return storage.Config{}
}

func (m *mockEngine) UpdateConfig(config storage.Config) error {
	return nil
}

func (m *mockEngine) GetIndex(tableName string, indexName string) (storage.Index, error) {
	return nil, nil
}

func (m *mockEngine) IndexExists(indexName, tableName string) (bool, error) {
	return false, nil
}

func (m *mockEngine) ListTableIndexes(tableName string) (map[string]string, error) {
	return nil, nil
}

// mockTransaction is a simple mock implementation of storage.Transaction
type mockTransaction struct{}

func (m *mockTransaction) Begin() error {
	return nil
}

func (m *mockTransaction) Commit() error {
	return nil
}

func (m *mockTransaction) Rollback() error {
	return nil
}

func (m *mockTransaction) GetTable(name string) (storage.Table, error) {
	return nil, nil
}

func (m *mockTransaction) CreateTable(name string, schema storage.Schema) (storage.Table, error) {
	return nil, nil
}

func (m *mockTransaction) DropTable(name string) error {
	return nil
}

func (m *mockTransaction) RenameTable(oldName, newName string) error {
	return nil
}

func (m *mockTransaction) CreateTableIndex(tableName, indexName string, columns []string, unique bool) error {
	return nil
}

func (m *mockTransaction) CreateTableColumnarIndex(tableName string, columnName string, isUnique bool, indexName ...string) error {
	return nil
}

func (m *mockTransaction) DropTableColumnarIndex(tableName string, columnName string) error {
	return nil
}

func (m *mockTransaction) DropTableIndex(tableName, indexName string) error {
	return nil
}

func (m *mockTransaction) DropTableColumn(tableName, columnName string) error {
	return nil
}

func (m *mockTransaction) AddTableColumn(tableName string, column storage.SchemaColumn) error {
	return nil
}

func (m *mockTransaction) RenameTableColumn(tableName, oldName, newName string) error {
	return nil
}

func (m *mockTransaction) Select(tableName string, columns []string, where *storage.Condition, originalColumns ...string) (storage.Result, error) {
	return nil, nil
}

func (m *mockTransaction) SelectWithAliases(tableName string, columns []string, where *storage.Condition, aliases map[string]string, originalColumns ...string) (storage.Result, error) {
	return nil, nil
}

func (m *mockTransaction) SelectWithExpression(tableName string, columns []string, where storage.Expression, expressions map[string]string, originalColumns ...string) (storage.Result, error) {
	return nil, nil
}

func (m *mockTransaction) Context() context.Context {
	return context.Background()
}

func (m *mockTransaction) ID() int64 {
	return 12345
}

func (m *mockTransaction) ListTables() ([]string, error) {
	return nil, nil
}

func (m *mockTransaction) ModifyTableColumn(tableName string, column storage.SchemaColumn) error {
	return nil
}
