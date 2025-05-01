package mvcc

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/semihalev/stoolap/internal/storage"
	"github.com/semihalev/stoolap/internal/storage/expression"
)

// Static counter to ensure uniqueness of row IDs
var autoIncrementCounter int64

// MVCCTable is a wrapper that provides MVCC isolation for tables
type MVCCTable struct {
	txnID        int64
	versionStore *VersionStore
	txnVersions  *TransactionVersionStore
	engine       *MVCCEngine
}

// Name returns the table name
func (mt *MVCCTable) Name() string {
	return mt.versionStore.tableName
}

// Schema returns the table schema
func (mt *MVCCTable) Schema() storage.Schema {
	// Get the schema from the engine
	if mt.versionStore == nil {
		return storage.Schema{}
	}

	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		// Return empty schema on error
		return storage.Schema{}
	}

	return schema
}

// CleanUp prepares an MVCCTable to be returned to a pool
// This helps reduce GC pressure by reusing MVCCTable objects
func (mt *MVCCTable) CleanUp() {
	// Only clean up transaction-specific data
	// Keep the reference to the underlying table and version store,
	// as these might be shared with the engine
	if mt.txnVersions != nil {
		mt.txnVersions.Rollback()
		mt.txnVersions = nil
	}
}

// Insert adds a new row to the table with MVCC isolation
func (mt *MVCCTable) Insert(row storage.Row) error {
	// Validate row before inserting
	if err := mt.validateRow(row); err != nil {
		return err
	}

	// Get schema for extracting row ID
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Extract row ID using the primary key
	rowID := extractRowPK(schema, row)

	// Fast path: Check if this rowID has been seen in this transaction
	// This avoids the expensive full Get operation
	if mt.txnVersions.HasLocallySeen(rowID) {
		// Need to do full check to handle deleted rows properly
		if _, exists := mt.txnVersions.Get(rowID); exists {
			return fmt.Errorf("primary key with %d already exists in this transaction", rowID)
		}
	}

	// Ultra-fast path: First check if the row exists at all in the version store
	// This avoids mutex acquisition and visibility checks for the common case
	// where the row doesn't exist at all
	if !mt.versionStore.QuickCheckRowExistence(rowID) {
		// Row definitely doesn't exist, skip expensive visibility check
	} else {
		// Row might exist, do the full visibility check
		if _, exists := mt.versionStore.GetVisibleVersion(rowID, mt.txnID); exists {
			return fmt.Errorf("primary key with %d already exists in this table", rowID)
		}
	}

	// Check unique columnar index constraints
	if err := mt.CheckUniqueConstraints(row); err != nil {
		return err
	}

	// Add to transaction's local version store using the numeric row ID
	mt.txnVersions.Put(rowID, row, false)

	return nil
}

// CheckUniqueConstraints checks if a row violates any unique columnar index constraints
// It returns ErrUniqueConstraintViolation if any constraints are violated
func (mt *MVCCTable) CheckUniqueConstraints(row storage.Row) error {
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	uniqueColumns := make(map[string]struct {
		index    *ColumnarIndex
		position int
	})

	// Get unique columnar indexes
	mt.versionStore.columnarMutex.RLock()
	for colName, index := range mt.versionStore.columnarIndexes {
		if colIdx, ok := index.(*ColumnarIndex); ok && colIdx.isUnique {
			// Find column position
			for i, col := range schema.Columns {
				if col.Name == colName {
					uniqueColumns[colName] = struct {
						index    *ColumnarIndex
						position int
					}{colIdx, i}
					break
				}
			}
		}
	}
	mt.versionStore.columnarMutex.RUnlock()

	if len(uniqueColumns) == 0 {
		return nil // No unique columns to check
	}

	// Check each unique column against all rows in the local transaction
	for _, col := range uniqueColumns {
		// Skip if column position out of bounds
		if col.position < 0 || col.position >= len(row) {
			continue
		}

		// Get the value we're trying to insert/update
		value := row[col.position]

		// Skip NULL values (they don't violate uniqueness)
		if value == nil || value.IsNull() {
			continue
		}

		// Check all rows in the local transaction
		for _, localRow := range mt.txnVersions.localVersions {
			if localRow.IsDeleted || localRow.Data == nil {
				continue // Skip deleted rows
			}

			// Ensure we have enough columns
			if len(localRow.Data) <= col.position {
				continue
			}

			// Check if this row has the same value
			localValue := localRow.Data[col.position]
			if localValue != nil && !localValue.IsNull() && localValue.Equals(value) {
				return storage.ErrUniqueConstraintViolation
			}
		}

		if col.index.HasUniqueValue(value) {
			return storage.ErrUniqueConstraintViolation
		}
	}

	return nil
}

// CheckIfAnyRowExists checks if any of the provided rowIDs already exist
// Returns the first existing rowID and true if any exist, otherwise 0 and false
// This avoids unnecessary allocations since we only need to know if ANY row exists
func (mt *MVCCTable) CheckIfAnyRowExists(rowIDs []int64) (int64, bool) {
	// First check local versions
	for _, rowID := range rowIDs {
		if mt.txnVersions.HasLocallySeen(rowID) {
			// Do full check for those that are locally seen
			if _, exists := mt.txnVersions.Get(rowID); exists {
				return rowID, true
			}
		}
	}

	// Then check global versions
	if mt.versionStore != nil {
		// First do a quick check without locking to filter out definite non-existent rows
		var possibleRowIDs []int64
		for _, rowID := range rowIDs {
			if mt.versionStore.QuickCheckRowExistence(rowID) {
				possibleRowIDs = append(possibleRowIDs, rowID)
			}
		}

		// If any rows might exist, do a full visibility check on just those rows
		if len(possibleRowIDs) > 0 {
			// Use a closure to capture the result
			var foundRowID int64
			var found bool

			mt.versionStore.IterateVisibleVersions(possibleRowIDs, mt.txnID,
				func(rowID int64, _ RowVersion) bool {
					foundRowID = rowID
					found = true
					return false // Stop iteration as soon as we find one
				})

			if found {
				return foundRowID, true
			}
		}
	}

	return 0, false
}

// InsertBatch adds multiple rows to the table with MVCC isolation
// This is an optimized version that reduces redundant checks for batch operations
func (mt *MVCCTable) InsertBatch(rows []storage.Row) error {
	// Quick exit for empty batch
	if len(rows) == 0 {
		return nil
	}

	// For small batches, just use the single row method to avoid overhead
	if len(rows) <= 3 {
		for _, row := range rows {
			if err := mt.Insert(row); err != nil {
				return err
			}
		}
		return nil
	}

	// Get schema information once for all rows
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Cache column types and constraints for faster validation
	columnTypes := make([]storage.DataType, len(schema.Columns))
	nullableFlags := make([]bool, len(schema.Columns))
	for i, col := range schema.Columns {
		columnTypes[i] = col.Type
		nullableFlags[i] = col.Nullable
	}

	// Validate all rows using cached schema info
	for i, row := range rows {
		// Perform fast validation with cached schema info
		if err := validateRowFast(row, columnTypes, nullableFlags); err != nil {
			return fmt.Errorf("validation error in row %d: %w", i, err)
		}
	}

	// Extract all row IDs first - reuse the schema we already fetched
	rowIDs := make([]int64, len(rows))
	for i, row := range rows {
		rowIDs[i] = extractRowPK(schema, row)
	}

	// Check if any row already exists (optimized to avoid allocations)
	if existingRowID, exists := mt.CheckIfAnyRowExists(rowIDs); exists {
		return fmt.Errorf("primary key with ID %d already exists", existingRowID)
	}

	// If we got here, none of the rows exist, so insert them all
	// Use the batch insertion method for better performance
	mt.txnVersions.PutRowsBatch(rowIDs, rows, false)

	return nil
}

// validateRow checks if a row is valid for the table's schema
func (mt *MVCCTable) validateRow(row storage.Row) error {
	// Check if the version store is nil
	if mt == nil || mt.versionStore == nil {
		return errors.New("invalid table: version store is nil")
	}

	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Check if row is nil
	if row == nil {
		return errors.New("invalid row: row is nil")
	}

	// Check column count
	if len(row) != len(schema.Columns) {
		return fmt.Errorf("invalid column count: expected %d, got %d", len(schema.Columns), len(row))
	}

	// Validate column types and NULL constraints
	for i, col := range schema.Columns {
		// Check if row[i] is nil
		if i >= len(row) || row[i] == nil {
			return fmt.Errorf("nil value at index %d (column '%s')", i, col.Name)
		}

		// Check NULL constraint
		if !col.Nullable && row[i].IsNull() {
			return fmt.Errorf("NULL value in non-nullable column '%s'", col.Name)
		}

		// Check type compatibility
		if !row[i].IsNull() {
			// First check the actual type - strict validation
			actualType := row[i].Type()
			if actualType != col.Type {
				return fmt.Errorf("type mismatch in column '%s': expected %v, got %v",
					col.Name, col.Type, actualType)
			}

			// Then also verify conversion works
			switch col.Type {
			case storage.TypeInteger:
				if _, ok := row[i].AsInt64(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Integer", col.Name)
				}
			case storage.TypeFloat:
				if _, ok := row[i].AsFloat64(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Float", col.Name)
				}
			case storage.TypeString:
				if _, ok := row[i].AsString(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected String", col.Name)
				}
			case storage.TypeBoolean:
				if _, ok := row[i].AsBoolean(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Boolean", col.Name)
				}
			case storage.TypeTimestamp:
				if _, ok := row[i].AsTimestamp(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Timestamp", col.Name)
				}
			case storage.TypeDate:
				if _, ok := row[i].AsDate(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Date", col.Name)
				}
			case storage.TypeTime:
				if _, ok := row[i].AsTime(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Time", col.Name)
				}
			case storage.TypeJSON:
				if _, ok := row[i].AsJSON(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected JSON", col.Name)
				}
			default:
				// For unknown or future types, just check the type matches
				if row[i].Type() != col.Type {
					return fmt.Errorf("type mismatch in column '%s': expected %v, got %v",
						col.Name, col.Type, row[i].Type())
				}
			}
		}
	}

	return nil
}

// validateRowFast performs a fast validation for batch operations
// using pre-computed schema type information for better performance
func validateRowFast(row storage.Row, columnTypes []storage.DataType, nullableFlags []bool) error {
	// Quick nil check
	if row == nil {
		return errors.New("invalid row: row is nil")
	}

	// Check column count
	if len(row) != len(columnTypes) {
		return fmt.Errorf("invalid column count: expected %d, got %d", len(columnTypes), len(row))
	}

	// Fast validation using cached type information
	for i, colType := range columnTypes {
		// Check if value is nil
		if i >= len(row) || row[i] == nil {
			return fmt.Errorf("nil value at index %d", i)
		}

		// Check NULL constraint
		if !nullableFlags[i] && row[i].IsNull() {
			return fmt.Errorf("NULL value in non-nullable column at index %d", i)
		}

		// Type check for non-NULL values
		if !row[i].IsNull() {
			// Fast type check - just compare the type ID
			// This avoids multiple method calls and string formatting
			actualType := row[i].Type()
			if actualType != colType {
				return fmt.Errorf("type mismatch at index %d: expected %v, got %v",
					i, colType, actualType)
			}
		}
	}

	return nil
}

// Update updates rows that match the expression
func (mt *MVCCTable) Update(where storage.Expression, setter func(storage.Row) storage.Row) (int, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)

	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize with our fast expressions
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return 0, nil
		}

		// For equality operator with PK, we can do direct update (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Direct lookup by ID
			row, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return 0, nil
			}

			// Apply the setter function
			updatedRow := setter(row)

			// Check unique columnar index constraints
			if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
				return 0, err
			}

			// Store the updated row
			mt.txnVersions.Put(pkInfo.ID, updatedRow, false)

			// Return count of 1 row updated
			return 1, nil
		}

		// For other operators, we'll use the optimized expression
		// but still need to perform a full scan
		if pkInfo.Expr != nil {
			where = pkInfo.Expr // Use optimized expression for evaluation
		}

		if len(pkInfos) == 2 {
			fastExpr1 := pkInfos[0].Expr
			fastExpr2 := pkInfos[1].Expr

			if (fastExpr1.Operator == storage.GT || fastExpr1.Operator == storage.GTE) &&
				(fastExpr2.Operator == storage.LT || fastExpr2.Operator == storage.LTE) {

				// Get range bounds with adjustments for inclusive/exclusive
				lowerBound := fastExpr1.Int64Value
				if fastExpr1.Operator == storage.GTE {
					lowerBound-- // Adjust for inclusive lower bound
				}

				upperBound := fastExpr2.Int64Value
				if fastExpr2.Operator == storage.LT {
					upperBound-- // Adjust for exclusive upper bound
				}

				// Build list of all potential IDs in the range
				visibleIDs := make([]int64, 0, upperBound-lowerBound)
				for id := lowerBound + 1; id <= upperBound; id++ {
					visibleIDs = append(visibleIDs, id)
				}

				// Process updates in batch
				updateCount := 0

				// First process locally visible rows (common case)
				localRows := make(map[int64]storage.Row)
				for _, id := range visibleIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists {
							localRows[id] = row
						}
					}
				}

				// Then get global versions for remaining IDs
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(visibleIDs, mt.txnID)
				defer ReturnVisibleVersionMap(globalRows)

				// Update local rows
				for id, row := range localRows {
					// Apply the setter function
					updatedRow := setter(row)

					// Check unique columnar index constraints
					if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
						return 0, err
					}

					// Store the updated row
					mt.txnVersions.Put(id, updatedRow, false)
					updateCount++
				}

				// Process global rows that weren't in local cache
				for id, version := range globalRows {
					if _, isLocal := localRows[id]; !isLocal && !version.IsDeleted {
						// Apply the setter function
						updatedRow := setter(version.Data)

						// Check unique columnar index constraints
						if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
							return 0, err
						}

						// Store the updated row
						mt.txnVersions.Put(id, updatedRow, false)
						updateCount++
					}
				}

				return updateCount, nil
			}
		}
	}

	// Try the columnar index optimization first for non-PK columns
	var schemaExpr *expression.SchemaAwareExpression

	// Prepare filter expression (optimized if possible)
	var filterExpr storage.Expression = where

	// Prepare schema-aware expression if not already optimized
	if where != nil {
		if _, ok := where.(*expression.FastSimpleExpression); ok {
			// Already optimized with FastSimpleExpression
			filterExpr = where
		} else if existingExpr, ok := where.(*expression.SchemaAwareExpression); ok {
			// Already schema-aware
			filterExpr = where
			schemaExpr = existingExpr
		} else {
			// Wrap with schema awareness for better column matching
			newExpr := expression.NewSchemaAwareExpression(where, schema)
			filterExpr = newExpr
			schemaExpr = newExpr
			defer expression.ReturnSchemaAwereExpressionPool(newExpr)
		}
	}

	// Try columnar index optimization if schema-aware expression is available
	if schemaExpr != nil {
		// Get filtered row IDs using columnar indexes
		rowIDs := mt.GetFilteredRowIDs(schemaExpr)

		if len(rowIDs) > 0 {
			// Processing update using columnar-index filtered row IDs
			updateCount := 0

			// Process in batches for better memory usage
			batchSize := 1000
			for i := 0; i < len(rowIDs); i += batchSize {
				end := i + batchSize
				if end > len(rowIDs) {
					end = len(rowIDs)
				}

				// Get this batch of rows
				batchIDs := rowIDs[i:end]
				versions := mt.versionStore.GetVisibleVersionsByIDs(batchIDs, mt.txnID)

				// Process each visible row in this batch
				for rowID, version := range versions {
					if !version.IsDeleted {
						// Apply the setter function
						updatedRow := setter(version.Data)

						// Check unique columnar index constraints
						if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
							return 0, err
						}

						// Store the updated row
						mt.txnVersions.Put(rowID, updatedRow, false)
						updateCount++
					}
				}

				// Free the versions map
				ReturnVisibleVersionMap(versions)
			}

			return updateCount, nil
		}
	}

	// Fall back to the general case if columnar index optimization didn't work
	processedKeys := GetProcessedKeysMap(100)
	defer PutProcessedKeysMap(processedKeys)

	// Count of rows updated
	updateCount := 0

	// PART 2: Process global versions with batch limiting
	processCount, err := mt.processGlobalVersions(filterExpr, processedKeys, func(rowID int64, row storage.Row) error {
		// Apply the setter function
		updatedRow := setter(row)

		// Check unique columnar index constraints
		if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
			return err
		}

		// Store the updated row
		mt.txnVersions.Put(rowID, updatedRow, false)

		return nil
	}, 1000) // Process in batches of 1000
	if err != nil {
		return 0, err
	}

	updateCount += processCount

	// PART 3: Process rows only visible in local versions for small updates
	if updateCount < 100 {
		// Skip this step for large updates to optimize memory usage
		allRows := mt.txnVersions.GetAllVisibleRows()
		for rowID, row := range allRows {
			// Skip already processed rows
			if processedKeys[rowID] {
				continue
			}

			// Skip if already marked as deleted locally
			if localVersion, exists := mt.txnVersions.localVersions[rowID]; exists && localVersion.IsDeleted {
				continue
			}

			// Apply filter if specified
			if !mt.matchesFilter(filterExpr, row) {
				continue
			}

			// Apply the setter function
			updatedRow := setter(row)

			// Check unique columnar index constraints
			if err := mt.CheckUniqueConstraints(updatedRow); err != nil {
				return 0, err
			}

			// Store the updated row
			mt.txnVersions.Put(rowID, updatedRow, false)

			updateCount++
		}

		// Return the map to the pool
		PutRowMap(allRows)
	}

	return updateCount, nil
}

// ProcessedKeysPool is a pool for bool maps to reduce allocations
var processedKeysPool = sync.Pool{
	New: func() interface{} {
		return make(map[int64]bool, 100) // Default to 100 capacity
	},
}

// GetProcessedKeysMap gets a map from the pool
func GetProcessedKeysMap(capacity int) map[int64]bool {
	mapInterface := processedKeysPool.Get()
	m, ok := mapInterface.(map[int64]bool)
	if !ok || m == nil {
		return make(map[int64]bool, capacity)
	}

	// Clear the map using Go 1.21+ built-in function
	clear(m)
	return m
}

// PutProcessedKeysMap returns a map to the pool
func PutProcessedKeysMap(m map[int64]bool) {
	if m == nil {
		return
	}

	// Clear the map
	clear(m)
	processedKeysPool.Put(m)
}

// matchesFilter evaluates if a row matches the filter expression
// Returns true if the row matches or if there's no filter
func (mt *MVCCTable) matchesFilter(expr storage.Expression, row storage.Row) bool {
	if expr == nil {
		return true // No filter, so everything matches
	}

	matches, err := expr.Evaluate(row)
	if err != nil {
		return false // Error in evaluation, consider as non-match
	}

	return matches
}

// processGlobalVersions processes visible versions from the global version store
// filter: The expression used for filtering (SchemaAwareExpression or FastSimpleExpression)
// processedKeys: Map of keys already processed to avoid duplicates
// processor: Function to call for each matching row
// batchSize: Maximum number of rows to process (0 for unlimited)
// Returns the number of rows processed
func (mt *MVCCTable) processGlobalVersions(
	filter storage.Expression,
	processedKeys map[int64]bool,
	processor func(int64, storage.Row) error,
	batchSize int,
) (int, error) {
	// Get visible versions from global store
	globalVersions := mt.versionStore.GetAllVisibleVersions(mt.txnID)
	defer ReturnVisibleVersionMap(globalVersions)

	processCount := 0

	// Process matching versions
	for rowID, version := range globalVersions {
		// Skip if already processed
		if processedKeys[rowID] {
			continue
		}

		// Skip deleted rows
		if version.IsDeleted {
			continue
		}

		// Apply filter
		if !mt.matchesFilter(filter, version.Data) {
			continue
		}

		// Mark as processed
		processedKeys[rowID] = true

		// Process this row
		err := processor(rowID, version.Data)
		if err != nil {
			return 0, err
		}

		processCount++

		// Check batch size limit
		if batchSize > 0 && processCount >= batchSize {
			break
		}
	}

	return processCount, nil
}

// Delete removes rows that match the expression
func (mt *MVCCTable) Delete(where storage.Expression) (int, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)
	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize with our fast expressions
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return 0, nil
		}

		// For equality operator with integer PK, we can do direct deletion (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Check if row exists and is visible
			_, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return 0, nil
			}

			// Mark the row as deleted with tombstone
			mt.txnVersions.Put(pkInfo.ID, nil, true)

			// Return count of 1 row deleted
			return 1, nil
		}

		// For other operators, we'll use the optimized expression
		// but still need to perform a full scan
		if pkInfo.Expr != nil {
			where = pkInfo.Expr // Use optimized expression for evaluation
		}

		// Fast path for range deletion
		if len(pkInfos) == 2 {
			fastExpr1 := pkInfos[0].Expr
			fastExpr2 := pkInfos[1].Expr

			if (fastExpr1.Operator == storage.GT || fastExpr1.Operator == storage.GTE) &&
				(fastExpr2.Operator == storage.LT || fastExpr2.Operator == storage.LTE) {

				// Get range bounds
				lowerBound := fastExpr1.Int64Value
				if fastExpr1.Operator == storage.GTE {
					lowerBound-- // Adjust for inclusive lower bound
				}

				upperBound := fastExpr2.Int64Value
				if fastExpr2.Operator == storage.LT {
					upperBound-- // Adjust for exclusive upper bound
				}

				// Prepare list of all IDs in the range to check
				visibleIDs := make([]int64, 0, upperBound-lowerBound)
				for id := lowerBound + 1; id <= upperBound; id++ {
					visibleIDs = append(visibleIDs, id)
				}

				// Process deletions in batch
				deleteCount := 0

				// Process local versions first (fast path)
				for _, id := range visibleIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists && row != nil {
							// Mark as deleted with tombstone
							mt.txnVersions.Put(id, nil, true)
							deleteCount++
						}
					}
				}

				// Then check global versions in bulk
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(visibleIDs, mt.txnID)
				defer ReturnVisibleVersionMap(globalRows)

				for id, version := range globalRows {
					if !version.IsDeleted {
						// Mark as deleted
						mt.txnVersions.Put(id, nil, true)
						deleteCount++
					}
				}

				return deleteCount, nil
			}
		}
	}

	// Try to use columnar indexes for optimization
	var schemaExpr *expression.SchemaAwareExpression

	// Prepare filter expression (optimized if possible)
	var filterExpr storage.Expression = where
	schema, err = mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Prepare schema-aware expression if not already optimized
	if where != nil {
		if _, ok := where.(*expression.FastSimpleExpression); ok {
			// Already optimized with FastSimpleExpression
			filterExpr = where
		} else if existingExpr, ok := where.(*expression.SchemaAwareExpression); ok {
			// Already schema-aware
			filterExpr = where
			schemaExpr = existingExpr
		} else {
			// Wrap with schema awareness for better column matching
			newExpr := expression.NewSchemaAwareExpression(where, schema)
			filterExpr = newExpr
			schemaExpr = newExpr
			defer expression.ReturnSchemaAwereExpressionPool(newExpr)
		}
	}

	// Try columnar index optimization if schema-aware expression is available
	if schemaExpr != nil {
		// Get filtered row IDs using columnar indexes
		rowIDs := mt.GetFilteredRowIDs(schemaExpr)

		if len(rowIDs) > 0 {
			// Process deletions in batches for better memory usage
			deleteCount := 0
			batchSize := 1000

			for i := 0; i < len(rowIDs); i += batchSize {
				end := i + batchSize
				if end > len(rowIDs) {
					end = len(rowIDs)
				}

				// Get this batch of rows
				batchIDs := rowIDs[i:end]

				// First process locally visible rows (fast path)
				for _, id := range batchIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists && row != nil {
							// Mark as deleted with tombstone
							mt.txnVersions.Put(id, nil, true)
							deleteCount++
						}
					}
				}

				// Then check global versions in bulk
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(batchIDs, mt.txnID)

				for id, version := range globalRows {
					if !version.IsDeleted {
						// Mark as deleted
						mt.txnVersions.Put(id, nil, true)
						deleteCount++
					}
				}

				// Free the versions map
				ReturnVisibleVersionMap(globalRows)
			}

			return deleteCount, nil
		}
	}

	// Use the pooled map for tracking processed keys
	processedKeys := GetProcessedKeysMap(100)
	defer PutProcessedKeysMap(processedKeys)

	// Count of rows deleted
	deleteCount := 0

	// Prepare schema-aware expression if not already optimized
	if where != nil {
		if _, ok := where.(*expression.FastSimpleExpression); ok {
			// Already optimized with FastSimpleExpression
			filterExpr = where
		} else if _, ok := where.(*expression.SchemaAwareExpression); ok {
			// Already schema-aware
			filterExpr = where
		} else {
			// Wrap with schema awareness for better column matching
			schemaExpr := expression.NewSchemaAwareExpression(where, schema)
			filterExpr = schemaExpr
			defer expression.ReturnSchemaAwereExpressionPool(schemaExpr)
		}
	}

	// PART 2: Process global versions with batch limiting
	processCount, _ := mt.processGlobalVersions(filterExpr, processedKeys, func(rowID int64, row storage.Row) error {
		// Mark as deleted in transaction's local versions
		mt.txnVersions.Put(rowID, nil, true)
		return nil
	}, 1000) // Process in batches of 1000

	deleteCount += processCount

	// PART 3: Process rows only visible in local versions for small deletes
	if deleteCount < 100 {
		// Skip this step for large deletes to optimize memory usage
		allRows := mt.txnVersions.GetAllVisibleRows()
		for rowID, row := range allRows {
			// Skip already processed rows
			if processedKeys[rowID] {
				continue
			}

			// Skip if already marked as deleted locally
			if localVersion, exists := mt.txnVersions.localVersions[rowID]; exists && localVersion.IsDeleted {
				continue
			}

			// Apply filter if specified
			if !mt.matchesFilter(filterExpr, row) {
				continue
			}

			// Mark as deleted
			mt.txnVersions.Put(rowID, nil, true)
			deleteCount++
		}

		// Return the map to the pool
		PutRowMap(allRows)
	}

	return deleteCount, nil
}

// Cleaned up old schemaCache code as we're now using schemaPKInfo instead

// Optimized PK cache with more metadata for faster extraction
type schemaPKInfo struct {
	pkIndices       []int                                      // Primary key column indices
	hasPK           bool                                       // Whether this schema has a primary key
	pkType          storage.DataType                           // Primary key data type (for single PKs)
	singleIntPK     bool                                       // Special flag for single integer PKs (fast path)
	singlePKIndex   int                                        // Index of the single PK column (for fast path)
	fastAccessFuncs []func(storage.ColumnValue) (uint64, bool) // Type-specific optimized access functions
}

// Cache of schema PK info to avoid expensive lookups
var schemaPKInfoCache sync.Map

// Internal constant for hash computation
const fnvPrime uint64 = 1099511628211
const fnvOffset uint64 = 14695981039346656037

// Precomputed fast access functions by column type
var pkAccessFuncs = map[storage.DataType]func(storage.ColumnValue) (uint64, bool){
	storage.TypeInteger: func(cv storage.ColumnValue) (uint64, bool) {
		if v, ok := cv.AsInt64(); ok {
			return uint64(v), true
		}
		return 0, false
	},
	storage.TypeFloat: func(cv storage.ColumnValue) (uint64, bool) {
		if v, ok := cv.AsFloat64(); ok {
			return math.Float64bits(v), true
		}
		return 0, false
	},
	storage.TypeBoolean: func(cv storage.ColumnValue) (uint64, bool) {
		if v, ok := cv.AsBoolean(); ok {
			if v {
				return 1, true
			}
			return 0, true
		}
		return 0, false
	},
	storage.TypeString: func(cv storage.ColumnValue) (uint64, bool) {
		// Fast path for DirectValue strings
		if dv, ok := cv.(*storage.DirectValue); ok && dv.AsInterface() != nil {
			if strVal, ok := dv.AsInterface().(string); ok {
				hash := fnvOffset
				for i := 0; i < len(strVal); i++ {
					hash ^= uint64(strVal[i])
					hash *= fnvPrime
				}
				return hash, true
			}
		}

		// Regular string path
		if strVal, ok := cv.AsString(); ok {
			hash := fnvOffset
			for i := 0; i < len(strVal); i++ {
				hash ^= uint64(strVal[i])
				hash *= fnvPrime
			}
			return hash, true
		}
		return 0, false
	},
}

// getOrCreatePKInfo gets or creates cached PK info for a schema
func getOrCreatePKInfo(schema storage.Schema) *schemaPKInfo {
	// Use table name as key instead of schema pointer
	// This is more stable across different schema instances with same content
	schemaKey := schema.TableName

	// Try fast path - check if already in cache
	if info, found := schemaPKInfoCache.Load(schemaKey); found {
		return info.(*schemaPKInfo)
	}

	// Create new PK info
	pkIndices := make([]int, 0, 4) // Most tables have few PK columns
	hasPK := false
	pkType := storage.NULL
	singleIntPK := false
	singlePKIndex := -1

	// Find primary key columns
	for i, col := range schema.Columns {
		if col.PrimaryKey {
			pkIndices = append(pkIndices, i)
			hasPK = true

			// Track if this is a single primary key
			if len(pkIndices) == 1 {
				pkType = col.Type
				singlePKIndex = i

				// Check if it's an integer PK (fastest path)
				if col.Type == storage.TypeInteger {
					singleIntPK = true
				}
			} else {
				// Multiple PKs - reset single PK flags
				singleIntPK = false
				singlePKIndex = -1
			}
		}
	}

	// If no primary key defined, use first column
	if !hasPK && len(schema.Columns) > 0 {
		pkIndices = append(pkIndices, 0)
		pkType = schema.Columns[0].Type
		singlePKIndex = 0
		if pkType == storage.TypeInteger {
			singleIntPK = true
		}
	}

	// Create fast access functions
	fastAccessFuncs := make([]func(storage.ColumnValue) (uint64, bool), len(pkIndices))
	for i, idx := range pkIndices {
		if idx < len(schema.Columns) {
			colType := schema.Columns[idx].Type
			if accessFunc, ok := pkAccessFuncs[colType]; ok {
				fastAccessFuncs[i] = accessFunc
			} else {
				// Default access function for unknown types
				fastAccessFuncs[i] = func(cv storage.ColumnValue) (uint64, bool) {
					return uint64(cv.Type()), false
				}
			}
		}
	}

	info := &schemaPKInfo{
		pkIndices:       pkIndices,
		hasPK:           hasPK,
		pkType:          pkType,
		singleIntPK:     singleIntPK,
		singlePKIndex:   singlePKIndex,
		fastAccessFuncs: fastAccessFuncs,
	}

	// Store in cache (if another thread did this simultaneously, we'll just have a duplicate that GC will clean up)
	schemaPKInfoCache.Store(schemaKey, info)
	return info
}

// extractRowPK extracts the primary key from a row, optimized for performance
func extractRowPK(schema storage.Schema, row storage.Row) int64 {
	// Get or create cached PK info
	pkInfo := getOrCreatePKInfo(schema)

	// Fast path: Single integer primary key (most common case)
	if pkInfo.singleIntPK && pkInfo.singlePKIndex >= 0 && pkInfo.singlePKIndex < len(row) {
		colVal := row[pkInfo.singlePKIndex]
		if colVal != nil && !colVal.IsNull() {
			// Direct fast access for integer PKs
			if intVal, ok := colVal.AsInt64(); ok && intVal > 0 {
				return intVal
			}
		}
	}

	// For non-integer or composite keys, compute hash
	var hash uint64 = fnvOffset

	// Use pre-computed access functions for better performance
	for i, idx := range pkInfo.pkIndices {
		if idx < len(row) && row[idx] != nil && !row[idx].IsNull() {
			// Add separator between values
			hash ^= 0xFF
			hash *= fnvPrime

			// Use optimized type-specific hash function
			if i < len(pkInfo.fastAccessFuncs) {
				if accessFunc := pkInfo.fastAccessFuncs[i]; accessFunc != nil {
					if val, ok := accessFunc(row[idx]); ok {
						hash ^= val
						hash *= fnvPrime
						continue
					}
				}
			}

			// Fallback to generic type handling
			switch row[idx].Type() {
			case storage.TypeInteger:
				if intVal, ok := row[idx].AsInt64(); ok {
					hash ^= uint64(intVal)
					hash *= fnvPrime
				}
			case storage.TypeFloat:
				if floatVal, ok := row[idx].AsFloat64(); ok {
					hash ^= math.Float64bits(floatVal)
					hash *= fnvPrime
				}
			case storage.TypeString:
				if strVal, ok := row[idx].AsString(); ok {
					for i := 0; i < len(strVal); i++ {
						hash ^= uint64(strVal[i])
						hash *= fnvPrime
					}
				}
			case storage.TypeBoolean:
				if boolVal, ok := row[idx].AsBoolean(); ok {
					if boolVal {
						hash ^= 1
					}
					hash *= fnvPrime
				}
			default:
				hash ^= uint64(row[idx].Type())
				hash *= fnvPrime
			}
		}
	}

	// Ensure positive int64
	rowHash := int64(hash & 0x7FFFFFFFFFFFFFFF)

	// Guarantee non-zero
	if rowHash == 0 {
		rowHash = 1
	}

	// Add uniqueness for tables without explicit PKs or with weak hashes
	if !pkInfo.hasPK || rowHash < 1000 {
		autoIncrement := atomic.AddInt64(&autoIncrementCounter, 1)
		rowHash = (rowHash * 10000) + autoIncrement
	}

	return rowHash
}

// Scan returns a scanner for rows in the table
func (mt *MVCCTable) Scan(columnIndices []int, where storage.Expression) (storage.Scanner, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)
	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize this with our fast expression
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return newEmptyScanner(), nil
		}

		// For equality operator, we can do direct row lookup (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Direct lookup by ID
			row, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return newEmptyScanner(), nil
			}

			// Return a scanner with just this single row
			return newSingleRowScanner(row, schema, columnIndices), nil
		}

		if pkInfo.Expr != nil {
			where = pkInfo.Expr
		}

		if len(pkInfos) == 2 {
			fastExpr1 := pkInfos[0].Expr
			fastExpr2 := pkInfos[1].Expr

			if (fastExpr1.Operator == storage.GT || fastExpr1.Operator == storage.GTE) &&
				(fastExpr2.Operator == storage.LT || fastExpr2.Operator == storage.LTE) {

				lowerBound := fastExpr1.Int64Value
				upperBound := fastExpr2.Int64Value

				// Adjust bounds based on inclusive/exclusive operators
				startID := lowerBound
				if fastExpr1.Operator == storage.GT {
					startID = lowerBound + 1 // exclusive lower bound
				}

				endID := upperBound
				inclusiveEnd := fastExpr2.Operator == storage.LTE

				// Create and return optimized range scanner
				return NewRangeScanner(
					mt.versionStore,
					startID,
					endID,
					inclusiveEnd,
					mt.txnID,
					schema,
					columnIndices,
				), nil
			}
		}
	}

	// Check if we can use columnar indexes for optimization
	var filterExpr storage.Expression
	if where != nil {
		// Ensure expression is schema-aware for columnar index usage
		if _, ok := where.(*expression.FastSimpleExpression); ok {
			// Already optimized, use as is
			filterExpr = where
		} else if schemaExpr, ok := where.(*expression.SchemaAwareExpression); ok {
			// Already schema-aware, use as is
			filterExpr = schemaExpr

			// Try to optimize with columnar indexes using the new direct row ID approach
			rowIDs := mt.GetFilteredRowIDs(schemaExpr)
			if len(rowIDs) > 0 {
				// Create the optimized columnar iterator for better performance
				return NewColumnarIndexIterator(
					mt.versionStore,
					rowIDs,
					mt.txnID,
					schema,
					columnIndices,
				), nil
			}
		}
	}

	// Regular path for all other queries
	// Get all visible rows with proper visibility checks for deleted rows
	visibleRows := mt.txnVersions.GetAllVisibleRows()

	// Create and return a scanner with the visible rows
	return NewMVCCScanner(visibleRows, schema, columnIndices, filterExpr), nil
}

// CreateColumn adds a column to the table
func (mt *MVCCTable) CreateColumn(name string, dataType storage.DataType, nullable bool) error {
	// Use the engine to perform the operation
	return mt.engine.CreateColumn(mt.versionStore.tableName, name, dataType, nullable)
}

// DropColumn removes a column from the table
func (mt *MVCCTable) DropColumn(name string) error {
	// Use the engine to perform the operation
	return mt.engine.DropColumn(mt.versionStore.tableName, name)
}

// CreateIndex creates an index on the table
func (mt *MVCCTable) CreateIndex(indexName string, columns []string, isUnique bool) error {
	// For now, we only support columnar indexes
	if len(columns) != 1 {
		return errors.New("only single-column indexes are supported")
	}

	// Create a columnar index for the column
	return mt.CreateColumnarIndex(columns[0], isUnique)
}

// DropIndex removes an index from the table
func (mt *MVCCTable) DropIndex(indexName string) error {
	// Extract column name from index name (columnar_tablename_columnname)
	parts := strings.Split(indexName, "_")
	if len(parts) < 3 || parts[0] != "columnar" {
		return fmt.Errorf("invalid index name format: %s", indexName)
	}

	// Get the column name from the index name
	columnName := parts[len(parts)-1]

	// Drop the columnar index
	return mt.DropColumnarIndex(columnName)
}

// Close closes the table
func (mt *MVCCTable) Close() error {
	// Just release resources
	mt.versionStore = nil
	mt.txnVersions = nil
	return nil
}

// PKOperationInfo contains the result of a primary key operation check
// with optimized information for fast path execution
type PKOperationInfo struct {
	// Primary key ID (for integer PKs)
	ID int64

	// Optimized expression for direct execution
	Expr *expression.FastSimpleExpression

	// Original comparison operator
	Operator storage.Operator

	// Whether this is a valid operation for fast path
	Valid bool

	// Special marker for empty result (contradictory conditions)
	EmptyResult bool
}

// GetPKOperationInfo optimizes and analyzes a filter expression for primary key operations
// Returns information that can be used for fast path execution
// It's especially useful for range narrowing in expressions like "id > 10 AND id < 20"
func GetPKOperationInfo(expr storage.Expression, schema storage.Schema) []PKOperationInfo {
	// Default result with invalid state
	result := PKOperationInfo{
		ID:          0,
		Expr:        nil,
		Operator:    storage.EQ,
		Valid:       false,
		EmptyResult: false,
	}

	// Quick exit for nil expressions
	if expr == nil {
		return []PKOperationInfo{result}
	}

	// Try to use our fast PK detector/optimizer
	fastExpr, ok := expression.FastPKDetector(expr, schema)
	if ok {
		// We have an optimized expression
		result.Expr = fastExpr
		result.Operator = fastExpr.Operator
		result.Valid = true

		// For integer PKs, also extract the ID for legacy code paths
		if fastExpr.ValueType == storage.TypeInteger {
			result.ID = fastExpr.Int64Value
		} else if fastExpr.ValueType == storage.TypeFloat {
			result.ID = int64(fastExpr.Float64Value)
		} else if fastExpr.ValueType == storage.TypeString {
			// Try to parse string as integer
			if id, err := strconv.ParseInt(fastExpr.StringValue, 10, 64); err == nil {
				result.ID = id
			}
		}

		return []PKOperationInfo{result}
	}

	// For complex AND expressions, check for special cases
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		// Try to extract operations on the same PK column
		pkInfoMap := make(map[string][]PKOperationInfo)

		// Extract PK info for all subexpressions
		for _, subExpr := range andExpr.Expressions {
			pkInfos := GetPKOperationInfo(subExpr, schema)
			if len(pkInfos) == 1 {
				if pkInfos[0].Valid && pkInfos[0].Expr != nil {
					pkInfoMap[pkInfos[0].Expr.Column] = append(pkInfoMap[pkInfos[0].Expr.Column], pkInfos[0])
				}
			}
		}

		// Look for contradictions or range expressions on the same PK
		for _, infoList := range pkInfoMap {
			if len(infoList) >= 2 {
				// Look for EQ operations on the same column
				eqValues := make(map[int64]bool)
				for _, info := range infoList {
					if info.Operator == storage.EQ && info.ID != 0 {
						eqValues[info.ID] = true
					}
				}

				if len(eqValues) > 1 {
					// Contradiction: x = 1 AND x = 2
					result.Valid = true
					result.EmptyResult = true
					return []PKOperationInfo{result}
				} else if len(eqValues) == 1 {
					// Multiple conditions but only one equality value
					// Return it as the canonical expression
					for _, info := range infoList {
						if info.Operator == storage.EQ {
							return []PKOperationInfo{result}
						}
					}
				}

				// Range narrowing for GT/LT combinations on integer PKs
				// This optimizes expressions like "id > 10 AND id < 20"
				var minBound, maxBound PKOperationInfo

				// First, extract any lower and upper bounds on integers
				for _, info := range infoList {
					// Only process integer expressions
					if info.Expr != nil && info.Expr.ValueType == storage.TypeInteger {
						if info.Operator == storage.GT || info.Operator == storage.GTE {
							// Lower bound (id > X or id >= X)
							if minBound.Expr == nil || info.Expr.Int64Value > minBound.Expr.Int64Value {
								minBound = info // Keep highest minimum value
							}
						} else if info.Operator == storage.LT || info.Operator == storage.LTE {
							// Upper bound (id < X or id <= X)
							if maxBound.Expr == nil || info.Expr.Int64Value < maxBound.Expr.Int64Value {
								maxBound = info // Keep lowest maximum value
							}
						}
					}
				}

				// Handle optimization for both upper and lower bounds
				// First, if we only have an upper bound, use that
				if minBound.Expr == nil && maxBound.Expr != nil {
					// Just return the upper bound as it's all we have
					return []PKOperationInfo{maxBound}
				}

				// If we only have a lower bound, use that
				if minBound.Expr != nil && maxBound.Expr == nil {
					// Just return the lower bound as it's all we have
					return []PKOperationInfo{minBound}
				}

				// If we have both bounds, check if we can optimize further
				if minBound.Expr != nil && maxBound.Expr != nil {
					// Get the actual bounds, adjusting for inclusive/exclusive
					lowerVal := minBound.Expr.Int64Value
					if minBound.Operator == storage.GT {
						lowerVal++ // For exclusive bounds (id > X), we add 1 to get the minimum possible value
					}

					upperVal := maxBound.Expr.Int64Value
					if maxBound.Operator == storage.LT {
						upperVal-- // For exclusive bounds (id < X), we subtract 1 to get the maximum possible value
					}

					// Check if bounds are contradictory (min > max)
					if lowerVal > upperVal {
						result.Valid = true
						result.EmptyResult = true
						return []PKOperationInfo{result}
					}

					// If the range narrows to an exact value (id >= 5 AND id <= 5)
					if lowerVal == upperVal {
						// Create an exact equality expression
						fastExpr := expression.NewFastSimpleExpression(
							minBound.Expr.Column,
							storage.EQ,
							lowerVal,
						)
						fastExpr.ColIndex = minBound.Expr.ColIndex
						fastExpr.IndexPrepped = true

						result.Expr = fastExpr
						result.Operator = storage.EQ
						result.Valid = true
						result.ID = lowerVal
						return []PKOperationInfo{result}
					}

					return []PKOperationInfo{minBound, maxBound}
				}
			}
		}
	}

	// Return default "not optimizable" result
	return []PKOperationInfo{result}
}

// singleRowScanner is a simple scanner that returns a single row
type singleRowScanner struct {
	row           storage.Row
	schema        storage.Schema
	columnIndices []int
	done          bool
}

// newSingleRowScanner creates a new scanner for a single row
func newSingleRowScanner(row storage.Row, schema storage.Schema, columnIndices []int) storage.Scanner {
	return &singleRowScanner{
		row:           row,
		schema:        schema,
		columnIndices: columnIndices,
		done:          false,
	}
}

// Next advances to the next row, returns false after first row
func (s *singleRowScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}

// Row returns the current row, with projection if needed
func (s *singleRowScanner) Row() storage.Row {
	if len(s.columnIndices) == 0 {
		return s.row
	}

	// Project the row to only include the requested columns
	projectedRow := make(storage.Row, len(s.columnIndices))
	for i, idx := range s.columnIndices {
		if idx < len(s.row) {
			projectedRow[i] = s.row[idx]
		}
	}
	return projectedRow
}

// Err returns any error that occurred
func (s *singleRowScanner) Err() error {
	return nil
}

// Close releases resources
func (s *singleRowScanner) Close() error {
	s.row = nil
	return nil
}

// emptyScanner is a scanner that returns no rows
type emptyScanner struct{}

// newEmptyScanner creates a new empty scanner
func newEmptyScanner() storage.Scanner {
	return &emptyScanner{}
}

// Next always returns false for empty scanner
func (s *emptyScanner) Next() bool {
	return false
}

// Row always returns nil for empty scanner
func (s *emptyScanner) Row() storage.Row {
	return nil
}

// Err always returns nil for empty scanner
func (s *emptyScanner) Err() error {
	return nil
}

// Close does nothing for empty scanner
func (s *emptyScanner) Close() error {
	return nil
}

// RowCount returns the number of rows in the table
func (mt *MVCCTable) RowCount() int {
	// Create set of all row IDs
	processedKeys := GetProcessedKeysMap(100)
	defer PutProcessedKeysMap(processedKeys)

	rowCount := 0

	// First grab visible versions from the version store
	// We don't need the full data, just how many rows are visible
	visibleVersions := mt.versionStore.GetAllVisibleVersions(mt.txnID)
	for rowID, version := range visibleVersions {
		if !version.IsDeleted {
			processedKeys[rowID] = true
			rowCount++
		}
	}
	ReturnVisibleVersionMap(visibleVersions)

	// Apply local changes that might override global versions
	for rowID, version := range mt.txnVersions.localVersions {
		if version.IsDeleted {
			// If deleted locally, remove from count
			if processedKeys[rowID] {
				processedKeys[rowID] = false
				rowCount--
			}
		} else if !processedKeys[rowID] {
			// If not already counted and not deleted
			processedKeys[rowID] = true
			rowCount++
		}
	}

	return rowCount
}

// Commit merges the transaction's local changes to the global version store
func (mt *MVCCTable) Commit() error {
	// Merge local changes into the global version store
	// The Commit method now automatically handles returning the object to the pool
	mt.txnVersions.Commit()
	mt.txnVersions = nil // Clear reference after commit
	return nil
}

// Rollback aborts the transaction and releases resources
func (mt *MVCCTable) Rollback() error {
	// Abort the transaction
	if mt.txnVersions != nil {
		mt.txnVersions.Rollback()
		mt.txnVersions = nil // Clear reference after rollback
	}
	return nil
}

// CreateColumnarIndex creates a columnar index for a column
// This provides HTAP capabilities by maintaining column-oriented indexes
// CreateColumnarIndex creates a columnar index for a column with optional uniqueness constraint
func (mt *MVCCTable) CreateColumnarIndex(columnName string, isUnique bool) error {
	// Check if version store is valid
	if mt.versionStore == nil {
		return fmt.Errorf("version store not available")
	}

	// Get schema to find column ID
	schema := mt.Schema()
	columnID := -1
	var dataType storage.DataType
	isPrimaryKey := false

	// Find column in schema
	for i, col := range schema.Columns {
		if col.Name == columnName {
			columnID = i
			dataType = col.Type
			isPrimaryKey = col.PrimaryKey
			break
		}
	}

	if columnID == -1 {
		return fmt.Errorf("column %s not found in schema", columnName)
	}

	// Prevent creating columnar indexes on primary key columns
	if isPrimaryKey {
		return fmt.Errorf("columnar indexes cannot be created on primary key columns. Use standard indexes instead")
	}

	// The version store's CreateColumnarIndex method already handles locking properly
	index, err := mt.versionStore.CreateColumnarIndex(mt.versionStore.tableName, columnName, columnID, dataType, isUnique)
	if err != nil {
		return err
	}

	// Record the creation in the WAL if persistence is enabled
	if mt.engine != nil && mt.engine.persistence != nil && mt.engine.persistence.IsEnabled() {
		// Get the column information for the index
		columnarIndex, ok := index.(*ColumnarIndex)
		if !ok {
			fmt.Printf("Warning: Index %s is not a columnar index, skipping WAL recording\n", index.Name())
			return nil
		}

		// Serialize the index metadata for WAL recording
		indexData, serErr := SerializeIndexMetadata(columnarIndex)
		if serErr != nil {
			fmt.Printf("Warning: Failed to serialize index metadata for WAL: %v\n", serErr)
			return nil
		}

		// Record in WAL using the dedicated index operation method
		err = mt.engine.persistence.RecordIndexOperation(
			mt.versionStore.tableName,
			WALCreateIndex,
			indexData, // Use the properly serialized index metadata
		)

		if err != nil {
			// Log the error but don't fail the operation since the index was already created
			fmt.Printf("Warning: Failed to record index creation in WAL: %v\n", err)
		}
	}

	return nil
}

// GetColumnarIndex retrieves a columnar index for a column if it exists
func (mt *MVCCTable) GetColumnarIndex(columnName string) (storage.Index, error) {
	// Check if the version store exists
	if mt.versionStore == nil {
		return nil, fmt.Errorf("version store not available")
	}

	// Get the index from the version store
	// The version store's GetColumnarIndex method already handles locking properly
	index, err := mt.versionStore.GetColumnarIndex(columnName)
	if err != nil {
		return nil, err
	}

	// Make sure it's a columnar index type
	if index.IndexType() != storage.ColumnarIndex {
		return nil, fmt.Errorf("index for column %s is not a columnar index", columnName)
	}

	return index, nil
}

// DropColumnarIndex removes a columnar index for a column
func (mt *MVCCTable) DropColumnarIndex(columnName string) error {
	// Check if version store is valid
	if mt.versionStore == nil {
		return fmt.Errorf("version store not available")
	}

	// First check if the index exists without holding the lock
	// This is a quick check that doesn't require the lock
	mt.versionStore.columnarMutex.RLock()
	_, exists := mt.versionStore.columnarIndexes[columnName]
	mt.versionStore.columnarMutex.RUnlock()

	if !exists {
		return fmt.Errorf("columnar index for column %s not found", columnName)
	}

	// Now acquire the write lock for the actual modification
	mt.versionStore.columnarMutex.Lock()
	defer mt.versionStore.columnarMutex.Unlock()

	// Re-check conditions after acquiring the lock
	// This is part of the double-checked locking pattern to handle
	// concurrent modifications that might have happened while we were waiting
	if _, exists := mt.versionStore.columnarIndexes[columnName]; !exists {
		return fmt.Errorf("columnar index for column %s not found", columnName)
	}

	// Get a reference to the index before removing it
	index := mt.versionStore.columnarIndexes[columnName]
	indexName := index.Name()

	// Remove the index from the map
	delete(mt.versionStore.columnarIndexes, columnName)

	// Attempt to close the index resources if it implements a Close method
	if closeableIndex, ok := index.(interface{ Close() error }); ok {
		_ = closeableIndex.Close() // Ignore errors during cleanup
	}

	// Record the drop in the WAL if persistence is enabled
	if mt.engine != nil && mt.engine.persistence != nil && mt.engine.persistence.IsEnabled() {
		// For drop operations, we still use the index name since we may not have
		// access to the full index data after deletion
		err := mt.engine.persistence.RecordIndexOperation(
			mt.versionStore.tableName,
			WALDropIndex,
			[]byte(indexName), // Store index name for drop operations
		)

		if err != nil {
			// Log the error but don't fail the operation since the index was already dropped
			fmt.Printf("Warning: Failed to record index drop in WAL: %v\n", err)
		}
	}

	return nil
}

// GetFilteredRowIDs extracts row IDs that match the expression using columnar indexes
// This is optimized for direct iteration over row IDs without materializing intermediate rows
func (mt *MVCCTable) GetFilteredRowIDs(schemaExpr *expression.SchemaAwareExpression) []int64 {
	// If the expression is nil or version store is not available, return empty result
	if schemaExpr == nil || mt.versionStore == nil {
		return nil
	}

	// Fast path for common case: direct simple expression on a single column
	if simpleExpr, ok := schemaExpr.Expr.(*expression.SimpleExpression); ok {
		// Check if we have an index for this column
		mt.versionStore.columnarMutex.RLock()
		_, indexExists := mt.versionStore.columnarIndexes[simpleExpr.Column]
		mt.versionStore.columnarMutex.RUnlock()

		if indexExists {
			// Get the index directly - if it fails, we'll fall back
			index, err := mt.GetColumnarIndex(simpleExpr.Column)
			if err == nil {
				// Use the optimized path for this index
				return GetRowIDsFromColumnarIndex(simpleExpr, index)
			}
		}
	}

	// Fast path for simple AND expression with two conditions
	if andExpr, ok := schemaExpr.Expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		if ok1 && ok2 {
			// Case 1: Conditions on the same column (e.g., x > 10 AND x < 20)
			if expr1.Column == expr2.Column {
				colName := expr1.Column

				// Check if we have an index for this column
				mt.versionStore.columnarMutex.RLock()
				_, indexExists := mt.versionStore.columnarIndexes[colName]
				mt.versionStore.columnarMutex.RUnlock()

				if indexExists {
					// Get the index directly
					index, err := mt.GetColumnarIndex(colName)
					if err == nil {
						// Let the index handle the AND condition directly
						return index.GetFilteredRowIDs(andExpr)
					}
				}
			} else {
				// Case 2: Conditions on different columns (e.g., x > 10 AND y = true)
				mt.versionStore.columnarMutex.RLock()
				_, index1Exists := mt.versionStore.columnarIndexes[expr1.Column]
				_, index2Exists := mt.versionStore.columnarIndexes[expr2.Column]
				mt.versionStore.columnarMutex.RUnlock()

				// If we have both indexes, we can apply them independently and intersect the results
				if index1Exists && index2Exists {
					// Get the first index
					index1, err1 := mt.GetColumnarIndex(expr1.Column)
					// Get the second index
					index2, err2 := mt.GetColumnarIndex(expr2.Column)

					if err1 == nil && err2 == nil {
						// Get matching row IDs from first condition
						rowIDs1 := GetRowIDsFromColumnarIndex(expr1, index1)

						// Get matching row IDs from second condition
						rowIDs2 := GetRowIDsFromColumnarIndex(expr2, index2)

						// If either result set is empty, return empty result
						if len(rowIDs1) == 0 || len(rowIDs2) == 0 {
							return nil
						}

						// Intersect the results and return
						return intersectSortedIDs(rowIDs1, rowIDs2)
					}
				}
			}
		}
	}

	// Try to extract column references to find usable indexes
	columnRefs := extractColumnReferences(schemaExpr)
	if len(columnRefs) == 0 {
		return nil
	}

	// Optimization: if there's only one column reference, retrieve directly
	if len(columnRefs) == 1 {
		colName := columnRefs[0]
		mt.versionStore.columnarMutex.RLock()
		_, exists := mt.versionStore.columnarIndexes[colName]
		mt.versionStore.columnarMutex.RUnlock()

		if exists {
			index, err := mt.GetColumnarIndex(colName)
			if err == nil {
				return GetRowIDsFromColumnarIndex(schemaExpr.Expr, index)
			}
		}
	}

	// Standard path for multiple conditions on different columns
	var matchingRowIDs []int64
	var foundIndex bool

	// Take a snapshot of available column indexes under a read lock
	// This avoids holding locks during expensive operations
	mt.versionStore.columnarMutex.RLock()
	availableIndexColumns := make([]string, 0, len(columnRefs))
	for _, colName := range columnRefs {
		if _, exists := mt.versionStore.columnarIndexes[colName]; exists {
			availableIndexColumns = append(availableIndexColumns, colName)
		}
	}
	mt.versionStore.columnarMutex.RUnlock()

	// Optimization: If no indexes are available, return early
	if len(availableIndexColumns) == 0 {
		return nil
	}

	// Try to find the most selective index first (the one likely to have the fewest matches)
	// This makes the intersection operations more efficient by starting with smaller sets
	if len(availableIndexColumns) > 1 {
		// Simple heuristic - use equality conditions first if available
		var mostSelectiveCol string
		var foundEquality bool

		for _, colName := range availableIndexColumns {
			// Simple check for equality conditions
			if hasEqualityCondition(schemaExpr.Expr, colName) {
				mostSelectiveCol = colName
				foundEquality = true
				break
			}
		}

		// If we found an equality condition, process it first
		if foundEquality {
			index, err := mt.GetColumnarIndex(mostSelectiveCol)
			if err == nil {
				rowIDs := GetRowIDsFromColumnarIndex(schemaExpr.Expr, index)
				if len(rowIDs) > 0 {
					matchingRowIDs = rowIDs
					foundIndex = true

					// Remove this column from the processing list
					for i := 0; i < len(availableIndexColumns); i++ {
						if availableIndexColumns[i] == mostSelectiveCol {
							availableIndexColumns = append(availableIndexColumns[:i], availableIndexColumns[i+1:]...)
							break
						}
					}
				}
			}
		}
	}

	// Process the remaining indexes
	for _, colName := range availableIndexColumns {
		// Skip if we've already processed this column
		if foundIndex && matchingRowIDs != nil && len(matchingRowIDs) == 0 {
			// Short circuit if we already have an empty result set
			return nil
		}

		// Try to get a columnar index for this column
		index, err := mt.GetColumnarIndex(colName)
		if err != nil {
			continue
		}

		// Use the index to get matching row IDs directly
		rowIDs := GetRowIDsFromColumnarIndex(schemaExpr.Expr, index)

		if len(rowIDs) > 0 {
			if !foundIndex {
				// First index found
				matchingRowIDs = rowIDs
				foundIndex = true
			} else {
				// Intersect with previous results using optimized intersection
				matchingRowIDs = intersectSortedIDs(matchingRowIDs, rowIDs)
			}
		}
	}

	return matchingRowIDs
}

// Helper function to check if an expression has an equality condition on a column
func hasEqualityCondition(expr storage.Expression, columnName string) bool {
	// Check simple expression
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		return simpleExpr.Column == columnName && simpleExpr.Operator == storage.EQ
	}

	// Check AND expression
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		for _, subExpr := range andExpr.Expressions {
			if hasEqualityCondition(subExpr, columnName) {
				return true
			}
		}
	}

	// Check OR expression
	if orExpr, ok := expr.(*expression.OrExpression); ok {
		for _, subExpr := range orExpr.Expressions {
			if hasEqualityCondition(subExpr, columnName) {
				return true
			}
		}
	}

	return false
}

// GetRowsWithFilter uses columnar indexes to filter rows based on an expression
// Returns a map of row IDs to rows that match the filter
func (mt *MVCCTable) GetRowsWithFilter(expr storage.Expression) map[int64]storage.Row {
	// Start with an empty result
	result := make(map[int64]storage.Row)

	// If the expression is nil or version store is not available, return empty result
	if expr == nil || mt.versionStore == nil {
		return result
	}

	// Extract schema-aware expression
	var schemaExpr *expression.SchemaAwareExpression
	var ok bool
	if schemaExpr, ok = expr.(*expression.SchemaAwareExpression); !ok {
		return result
	}

	// Get filtered row IDs using our optimized method
	matchingRowIDs := mt.GetFilteredRowIDs(schemaExpr)

	// If we found matching rows, fetch them from the version store
	if len(matchingRowIDs) > 0 {
		// For efficiency, use the batch version retrieval if there are many rows
		if len(matchingRowIDs) > 10 {
			// Get visible versions in bulk for efficiency
			versions := mt.versionStore.GetVisibleVersionsByIDs(matchingRowIDs, mt.txnID)
			defer ReturnVisibleVersionMap(versions)

			// Process the visible versions
			for rowID, version := range versions {
				if !version.IsDeleted {
					result[rowID] = version.Data
				}
			}
		} else {
			// For smaller sets, get rows individually
			for _, rowID := range matchingRowIDs {
				version, exists := mt.versionStore.GetVisibleVersion(rowID, mt.txnID)
				if exists && !version.IsDeleted {
					result[rowID] = version.Data
				}
			}
		}
	}

	return result
}

func extractColumnReferences(expr *expression.SchemaAwareExpression) []string {
	columnSet := make(map[string]bool)

	// Helper function to process a single expression
	var processExpression func(e storage.Expression)
	processExpression = func(e storage.Expression) {
		if e == nil {
			return
		}

		// Try different expression types to extract column references
		switch typedExpr := e.(type) {
		case *expression.SimpleExpression:
			// Simple expressions directly reference columns
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.AndExpression:
			// Process each child expression of AND
			for _, child := range typedExpr.Expressions {
				processExpression(child)
			}

		case *expression.OrExpression:
			// Process each child expression of OR
			for _, child := range typedExpr.Expressions {
				processExpression(child)
			}

		case *expression.BetweenExpression:
			// BETWEEN expressions reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.InListExpression:
			// IN expressions reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.SchemaAwareExpression:
			// Process the wrapped expression
			if typedExpr.Expr != nil {
				processExpression(typedExpr.Expr)
			}
		}
	}

	// Process the top-level expression
	if expr != nil && expr.Expr != nil {
		processExpression(expr.Expr)
	}

	// Convert set to slice
	var result []string

	for col := range columnSet {
		result = append(result, col)
	}

	return result
}
