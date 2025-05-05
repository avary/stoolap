package storage

import (
	"errors"
	"fmt"
)

var (
	// ErrTableNotFound is returned when a table is not found
	ErrTableNotFound = errors.New("table not found")
	// ErrTableAlreadyExists is returned when a table already exists
	ErrTableAlreadyExists = errors.New("table already exists")
	// ErrColumnNotFound is returned when a column is not found
	ErrColumnNotFound = errors.New("column not found")
	// ErrInvalidColumnType is returned when a column type is invalid
	ErrInvalidColumnType = errors.New("invalid column type")
	// ErrTransactionNotStarted is returned when a transaction is not started
	ErrTransactionNotStarted = errors.New("transaction not started")
	// ErrTransactionAlreadyStarted is returned when a transaction is already started
	ErrTransactionAlreadyStarted = errors.New("transaction already started")
	// ErrInvalidValue is returned when a value is invalid
	ErrInvalidValue = errors.New("invalid value")
	// ErrNotSupported is returned when an operation is not supported
	ErrNotSupported = errors.New("operation not supported")

	ErrTableClosed          = errors.New("table closed")
	ErrDuplicateColumn      = errors.New("duplicate column")
	ErrSegmentNotFound      = errors.New("segment not found")
	ErrExprEvaluation       = errors.New("expression evaluation failed")
	ErrTransactionEnded     = errors.New("transaction already ended")
	ErrTransactionAborted   = errors.New("transaction aborted")
	ErrTransactionCommitted = errors.New("transaction already committed")
	ErrTransactionClosed    = errors.New("transaction already closed")

	// Index-related errors
	// ErrIndexNotFound is returned when an index is not found
	ErrIndexNotFound = errors.New("index not found")
	// ErrIndexAlreadyExists is returned when an index already exists
	ErrIndexAlreadyExists = errors.New("index already exists")
	// ErrIndexColumnNotFound is returned when a column specified for an index is not found
	ErrIndexColumnNotFound = errors.New("index column not found")

	// Value comparison errors
	// ErrNullComparison is returned when trying to compare NULL values
	ErrNullComparison = errors.New("cannot compare NULL with non-NULL value")
	// ErrIncomparableTypes is returned when trying to compare incompatible types
	ErrIncomparableTypes = errors.New("cannot compare incompatible types")
)

// ErrTableColumnsNotMatch is returned when table columns don't match
type ErrTableColumnsNotMatch struct {
	Expected int
	Got      int
}

func (e ErrTableColumnsNotMatch) Error() string {
	return fmt.Sprintf("table columns don't match, expected %d, got %d", e.Expected, e.Got)
}

// ErrValueTooLong is returned when a value is too long
type ErrValueTooLong struct {
	Column string
	Max    int
	Got    int
}

func (e ErrValueTooLong) Error() string {
	return fmt.Sprintf("value for column %s is too long, max %d, got %d", e.Column, e.Max, e.Got)
}

// ErrNotNullConstraint is returned when a not null constraint is violated
type ErrNotNullConstraint struct {
	Column string
}

func (e ErrNotNullConstraint) Error() string {
	return fmt.Sprintf("not null constraint failed for column %s", e.Column)
}

// ErrPrimaryKeyConstraint is returned when a primary key constraint is violated
type ErrPrimaryKeyConstraint struct {
	RowID int64
}

func (e ErrPrimaryKeyConstraint) Error() string {
	return fmt.Sprintf("primary key constraint failed with %d already exists in this table", e.RowID)
}

// NewPrimaryKeyConstraintError creates a new primary key error
func NewPrimaryKeyConstraintError(rowID int64) error {
	return &ErrPrimaryKeyConstraint{
		RowID: rowID,
	}
}

// ErrUniqueConstraint is returned when a unique constraint is violated
type ErrUniqueConstraint struct {
	Index  string
	Column string
	Value  ColumnValue
}

func (e ErrUniqueConstraint) Error() string {
	return fmt.Sprintf("unique constraint failed for index %s on column %s with value %v", e.Index, e.Column, e.Value.AsInterface())
}

// NewUniqueConstraintError creates a new unique constraint error
func NewUniqueConstraintError(indexName, column string, value ColumnValue) error {
	return &ErrUniqueConstraint{
		Index:  indexName,
		Column: column,
		Value:  value,
	}
}
