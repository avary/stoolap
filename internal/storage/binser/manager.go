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

package binser

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MetadataManager provides a centralized system for managing metadata
// with efficient binary serialization and caching.
type MetadataManager struct {
	basePath    string
	cacheMutex  sync.RWMutex
	engineMeta  *EngineMetadata
	tableCache  map[string]*TableMetadata
	columnCache map[string]map[string]*ColumnMetadata
	indexCache  map[string]map[string]*IndexMetadata
	dirty       map[string]bool // Track which tables need to be written back
	engineDirty bool            // Track if engine metadata needs to be written back
}

var (
	ErrMetadataNotFound = errors.New("metadata not found")
	ErrInvalidPath      = errors.New("invalid metadata path")
)

// NewMetadataManager creates a new metadata manager for the given base path
func NewMetadataManager(basePath string) (*MetadataManager, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	manager := &MetadataManager{
		basePath:    basePath,
		tableCache:  make(map[string]*TableMetadata),
		columnCache: make(map[string]map[string]*ColumnMetadata),
		indexCache:  make(map[string]map[string]*IndexMetadata),
		dirty:       make(map[string]bool),
		engineDirty: false,
	}

	// Try to load engine metadata if it exists
	engineMeta, err := manager.GetEngineMetadata()
	if err != nil && !errors.Is(err, ErrMetadataNotFound) {
		return nil, fmt.Errorf("failed to load engine metadata: %w", err)
	}

	// If no engine metadata exists yet, create a default
	if engineMeta == nil {
		manager.engineMeta = &EngineMetadata{
			Tables:    make([]string, 0),
			UpdatedAt: time.Now(),
		}
		manager.engineDirty = true
	}

	return manager, nil
}

// getMetadataPath returns the path to the binary metadata file for a table
func (m *MetadataManager) getMetadataPath(tableName string) string {
	return filepath.Join(m.basePath, "tables", tableName, "metadata.bin")
}

// getEngineMetadataPath returns the path to the engine metadata file
func (m *MetadataManager) getEngineMetadataPath() string {
	return filepath.Join(m.basePath, "metadata.bin")
}

// getColumnMetadataPath returns the path to a column's metadata file
func (m *MetadataManager) getColumnMetadataPath(tableName, columnName string) string {
	return filepath.Join(m.basePath, "tables", tableName, "columns", columnName, "metadata.bin")
}

// getIndexMetadataPath returns the path to an index's metadata file
func (m *MetadataManager) getIndexMetadataPath(tableName, indexName string) string {
	return filepath.Join(m.basePath, "tables", tableName, "indexes", indexName, "metadata.bin")
}

// GetTableMetadata retrieves the metadata for a table
func (m *MetadataManager) GetTableMetadata(tableName string) (*TableMetadata, error) {
	// Check the cache first
	m.cacheMutex.RLock()
	if meta, ok := m.tableCache[tableName]; ok {
		m.cacheMutex.RUnlock()
		return meta, nil
	}
	m.cacheMutex.RUnlock()

	// Not in cache, need to read from disk
	path := m.getMetadataPath(tableName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrMetadataNotFound
		}
		return nil, err
	}

	// Try to deserialize the metadata
	meta := &TableMetadata{}
	reader := NewReader(data)
	if err := meta.UnmarshalBinary(reader); err != nil {
		// Check for backup before giving up
		backupPath := path + ".bak"
		backupData, backupErr := os.ReadFile(backupPath)
		if backupErr == nil {
			// Try to parse the backup
			backupReader := NewReader(backupData)
			backupMeta := &TableMetadata{}
			if backupErr := backupMeta.UnmarshalBinary(backupReader); backupErr == nil {
				// Successfully restored from backup - try to save it as the primary
				if writeErr := os.WriteFile(path, backupData, 0644); writeErr == nil {
					// Cache the restored metadata
					m.cacheMutex.Lock()
					m.tableCache[tableName] = backupMeta
					m.cacheMutex.Unlock()

					return backupMeta, nil
				}

				// Cache the restored metadata
				m.cacheMutex.Lock()
				m.tableCache[tableName] = backupMeta
				m.cacheMutex.Unlock()

				return backupMeta, nil
			}
		}

		// No successful recovery, return the original error
		return nil, err
	}

	// Cache the metadata
	m.cacheMutex.Lock()
	m.tableCache[tableName] = meta
	m.cacheMutex.Unlock()

	return meta, nil
}

// SaveTableMetadata saves table metadata to disk
func (m *MetadataManager) SaveTableMetadata(meta *TableMetadata) error {
	if meta == nil || meta.Name == "" {
		return ErrInvalidPath
	}

	// Serialize the metadata
	writer := NewWriter()
	defer writer.Release()
	meta.MarshalBinary(writer)
	data := writer.Bytes()

	// Make sure the directory exists
	tablePath := filepath.Join(m.basePath, "tables", meta.Name)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return err
	}

	// Write the file atomically
	path := m.getMetadataPath(meta.Name)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Update the cache
	m.cacheMutex.Lock()
	m.tableCache[meta.Name] = meta
	m.dirty[meta.Name] = false // Mark as clean after save
	m.cacheMutex.Unlock()

	return nil
}

// GetColumnMetadata retrieves metadata for a column
func (m *MetadataManager) GetColumnMetadata(tableName, columnName string) (*ColumnMetadata, error) {
	// Check the cache first
	m.cacheMutex.RLock()
	if tableColumns, ok := m.columnCache[tableName]; ok {
		if meta, ok := tableColumns[columnName]; ok {
			m.cacheMutex.RUnlock()
			return meta, nil
		}
	}
	m.cacheMutex.RUnlock()

	// Not in cache, need to read from disk
	path := m.getColumnMetadataPath(tableName, columnName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrMetadataNotFound
		}
		return nil, err
	}

	// Try to deserialize the metadata
	meta := &ColumnMetadata{}
	reader := NewReader(data)
	if err := meta.UnmarshalBinary(reader); err != nil {
		// Check for backup before giving up
		backupPath := path + ".bak"
		backupData, backupErr := os.ReadFile(backupPath)
		if backupErr == nil {
			// Try to parse the backup
			backupReader := NewReader(backupData)
			backupMeta := &ColumnMetadata{}
			if backupErr := backupMeta.UnmarshalBinary(backupReader); backupErr == nil {
				// Successfully restored from backup - try to save it as the primary
				if writeErr := os.WriteFile(path, backupData, 0644); writeErr == nil {
					// Cache the restored metadata
					m.cacheMutex.Lock()
					if _, ok := m.columnCache[tableName]; !ok {
						m.columnCache[tableName] = make(map[string]*ColumnMetadata)
					}
					m.columnCache[tableName][columnName] = backupMeta
					m.cacheMutex.Unlock()

					return backupMeta, nil
				}

				// Cache the restored metadata
				m.cacheMutex.Lock()
				if _, ok := m.columnCache[tableName]; !ok {
					m.columnCache[tableName] = make(map[string]*ColumnMetadata)
				}
				m.columnCache[tableName][columnName] = backupMeta
				m.cacheMutex.Unlock()

				return backupMeta, nil
			}
		}

		// No successful recovery, return the original error
		return nil, err
	}

	// Cache the metadata
	m.cacheMutex.Lock()
	if _, ok := m.columnCache[tableName]; !ok {
		m.columnCache[tableName] = make(map[string]*ColumnMetadata)
	}
	m.columnCache[tableName][columnName] = meta
	m.cacheMutex.Unlock()

	return meta, nil
}

// SaveColumnMetadata saves column metadata to disk
func (m *MetadataManager) SaveColumnMetadata(tableName string, meta *ColumnMetadata) error {
	if meta == nil || meta.Name == "" {
		return ErrInvalidPath
	}

	// Serialize the metadata
	writer := NewWriter()
	defer writer.Release()
	meta.MarshalBinary(writer)
	data := writer.Bytes()

	// Make sure the directory exists
	columnPath := filepath.Join(m.basePath, "tables", tableName, "columns", meta.Name)
	if err := os.MkdirAll(columnPath, 0755); err != nil {
		return err
	}

	// Write the file atomically
	path := m.getColumnMetadataPath(tableName, meta.Name)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Update the cache
	m.cacheMutex.Lock()
	if _, ok := m.columnCache[tableName]; !ok {
		m.columnCache[tableName] = make(map[string]*ColumnMetadata)
	}
	m.columnCache[tableName][meta.Name] = meta
	m.cacheMutex.Unlock()

	return nil
}

// GetIndexMetadata retrieves metadata for an index
func (m *MetadataManager) GetIndexMetadata(tableName, indexName string) (*IndexMetadata, error) {
	// Check the cache first
	m.cacheMutex.RLock()
	if tableIndices, ok := m.indexCache[tableName]; ok {
		if meta, ok := tableIndices[indexName]; ok {
			m.cacheMutex.RUnlock()
			return meta, nil
		}
	}
	m.cacheMutex.RUnlock()

	// Not in cache, need to read from disk
	path := m.getIndexMetadataPath(tableName, indexName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrMetadataNotFound
		}
		return nil, err
	}

	// Deserialize the metadata
	meta := &IndexMetadata{}
	reader := NewReader(data)
	if err := meta.UnmarshalBinary(reader); err != nil {
		return nil, err
	}

	// Cache the metadata
	m.cacheMutex.Lock()
	if _, ok := m.indexCache[tableName]; !ok {
		m.indexCache[tableName] = make(map[string]*IndexMetadata)
	}
	m.indexCache[tableName][indexName] = meta
	m.cacheMutex.Unlock()

	return meta, nil
}

// SaveIndexMetadata saves index metadata to disk
func (m *MetadataManager) SaveIndexMetadata(tableName string, meta *IndexMetadata) error {
	if meta == nil || meta.Name == "" {
		return ErrInvalidPath
	}

	// Serialize the metadata
	writer := NewWriter()
	defer writer.Release()
	meta.MarshalBinary(writer)
	data := writer.Bytes()

	// Make sure the directory exists
	indexPath := filepath.Join(m.basePath, "tables", tableName, "indexes", meta.Name)
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return err
	}

	// Write the file atomically
	path := m.getIndexMetadataPath(tableName, meta.Name)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Update the cache
	m.cacheMutex.Lock()
	if _, ok := m.indexCache[tableName]; !ok {
		m.indexCache[tableName] = make(map[string]*IndexMetadata)
	}
	m.indexCache[tableName][meta.Name] = meta
	m.cacheMutex.Unlock()

	return nil
}

// DeleteTableMetadata deletes a table's metadata and removes it from cache
func (m *MetadataManager) DeleteTableMetadata(tableName string) error {
	// Remove from disk - entire table directory
	tablePath := filepath.Join(m.basePath, "tables", tableName)
	if err := os.RemoveAll(tablePath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	// Remove from cache
	m.cacheMutex.Lock()
	delete(m.tableCache, tableName)
	delete(m.columnCache, tableName)
	delete(m.indexCache, tableName)
	delete(m.dirty, tableName)
	m.cacheMutex.Unlock()

	return nil
}

// DeleteColumnMetadata deletes a column's metadata and removes it from cache
func (m *MetadataManager) DeleteColumnMetadata(tableName, columnName string) error {
	// Remove from disk - column directory
	columnPath := filepath.Join(m.basePath, "tables", tableName, "columns", columnName)
	if err := os.RemoveAll(columnPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	// Remove from cache
	m.cacheMutex.Lock()
	if tableColumns, ok := m.columnCache[tableName]; ok {
		delete(tableColumns, columnName)
	}
	m.cacheMutex.Unlock()

	return nil
}

// DeleteIndexMetadata deletes an index's metadata and removes it from cache
func (m *MetadataManager) DeleteIndexMetadata(tableName, indexName string) error {
	// Remove from disk - index directory
	indexPath := filepath.Join(m.basePath, "tables", tableName, "indexes", indexName)
	if err := os.RemoveAll(indexPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	// Remove from cache
	m.cacheMutex.Lock()
	if tableIndices, ok := m.indexCache[tableName]; ok {
		delete(tableIndices, indexName)
	}
	m.cacheMutex.Unlock()

	return nil
}

// MarkDirty marks a table's metadata as modified, needing to be saved
func (m *MetadataManager) MarkDirty(tableName string) {
	m.cacheMutex.Lock()
	m.dirty[tableName] = true
	m.cacheMutex.Unlock()
}

// GetEngineMetadata retrieves the metadata for the engine
func (m *MetadataManager) GetEngineMetadata() (*EngineMetadata, error) {
	m.cacheMutex.RLock()
	if m.engineMeta != nil {
		meta := m.engineMeta
		m.cacheMutex.RUnlock()
		return meta, nil
	}
	m.cacheMutex.RUnlock()

	// Not in cache, try to read from disk
	path := m.getEngineMetadataPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrMetadataNotFound
		}
		return nil, err
	}

	// Deserialize the metadata
	meta := &EngineMetadata{}
	reader := NewReader(data)
	if err := meta.UnmarshalBinary(reader); err != nil {
		return nil, err
	}

	// Cache the metadata
	m.cacheMutex.Lock()
	m.engineMeta = meta
	m.cacheMutex.Unlock()

	return meta, nil
}

// SaveEngineMetadata saves engine metadata to disk
func (m *MetadataManager) SaveEngineMetadata(meta *EngineMetadata) error {
	if meta == nil {
		return ErrInvalidPath
	}

	// Serialize the metadata
	writer := NewWriter()
	defer writer.Release()
	meta.MarshalBinary(writer)
	data := writer.Bytes()

	// Write the file atomically
	path := m.getEngineMetadataPath()
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Update the cache
	m.cacheMutex.Lock()
	m.engineMeta = meta
	m.engineDirty = false
	m.cacheMutex.Unlock()

	return nil
}

// UpdateEngineMetadata updates the engine metadata with current table list
func (m *MetadataManager) UpdateEngineMetadata(tableNames []string) error {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	// If we don't have engine metadata yet, create it
	if m.engineMeta == nil {
		m.engineMeta = &EngineMetadata{
			Tables:    make([]string, len(tableNames)),
			UpdatedAt: time.Now(),
		}
		copy(m.engineMeta.Tables, tableNames)
	} else {
		// Otherwise update the existing metadata
		m.engineMeta.Tables = make([]string, len(tableNames))
		copy(m.engineMeta.Tables, tableNames)
		m.engineMeta.UpdatedAt = time.Now()
	}

	m.engineDirty = true
	return nil
}

// Flush writes all modified metadata to disk
func (m *MetadataManager) Flush() error {
	m.cacheMutex.Lock()
	dirtyTables := make([]string, 0, len(m.dirty))
	for tableName, isDirty := range m.dirty {
		if isDirty {
			dirtyTables = append(dirtyTables, tableName)
		}
	}
	engineDirty := m.engineDirty
	m.cacheMutex.Unlock()

	var firstErr error

	// Save dirty table metadata
	for _, tableName := range dirtyTables {
		m.cacheMutex.RLock()
		meta := m.tableCache[tableName]
		m.cacheMutex.RUnlock()

		if meta != nil {
			if err := m.SaveTableMetadata(meta); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("error saving metadata for table %s: %w", tableName, err)
			}
		}
	}

	// Save engine metadata if dirty
	if engineDirty {
		m.cacheMutex.RLock()
		meta := m.engineMeta
		m.cacheMutex.RUnlock()

		if meta != nil {
			if err := m.SaveEngineMetadata(meta); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("error saving engine metadata: %w", err)
			}
		}
	}

	return firstErr
}

// GetAllIndexMetadata returns all index metadata for a table
func (m *MetadataManager) GetAllIndexMetadata(tableName string) ([]*IndexMetadata, error) {
	// Check if we have any cached indexes for this table
	m.cacheMutex.RLock()
	tableIndices, ok := m.indexCache[tableName]
	m.cacheMutex.RUnlock()

	// If we have cached indexes, return them
	if ok && len(tableIndices) > 0 {
		indices := make([]*IndexMetadata, 0, len(tableIndices))
		for _, meta := range tableIndices {
			indices = append(indices, meta)
		}
		return indices, nil
	}

	// Otherwise, scan the index directory on disk
	indexesDir := filepath.Join(m.basePath, "tables", tableName, "indexes")
	if _, err := os.Stat(indexesDir); os.IsNotExist(err) {
		// No indexes directory, return empty list
		return []*IndexMetadata{}, nil
	}

	// Read all subdirectories in the indexes directory
	entries, err := os.ReadDir(indexesDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read indexes directory: %w", err)
	}

	// Process each index
	indices := make([]*IndexMetadata, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue // Skip non-directories
		}

		// Try to load the index metadata
		indexName := entry.Name()
		meta, err := m.GetIndexMetadata(tableName, indexName)
		if err != nil {
			continue
		}

		indices = append(indices, meta)
	}

	return indices, nil
}

// Close flushes all cached metadata to disk and releases resources
func (m *MetadataManager) Close() error {
	return m.Flush()
}

// getStatisticsMetadataPath returns the path to the statistics metadata file
func (m *MetadataManager) getStatisticsMetadataPath(tableName string) string {
	return filepath.Join(m.basePath, "statistics", tableName, "metadata.bin")
}

// SaveStatisticsMetadata saves statistics metadata for a table
func (m *MetadataManager) SaveStatisticsMetadata(stats *StatisticsMetadata) error {
	if stats == nil || stats.TableName == "" {
		return ErrInvalidPath
	}

	// Serialize the metadata
	writer := NewWriter()
	defer writer.Release()
	stats.MarshalBinary(writer)
	data := writer.Bytes()

	// Make sure the directory exists
	statsPath := filepath.Join(m.basePath, "statistics", stats.TableName)
	if err := os.MkdirAll(statsPath, 0755); err != nil {
		return err
	}

	// Write the file atomically
	path := m.getStatisticsMetadataPath(stats.TableName)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	return nil
}

// GetStatisticsMetadata gets statistics metadata for a table
func (m *MetadataManager) GetStatisticsMetadata(tableName string) (*StatisticsMetadata, error) {
	// Read from disk
	path := m.getStatisticsMetadataPath(tableName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrMetadataNotFound
		}
		return nil, err
	}

	// Deserialize the metadata
	stats := &StatisticsMetadata{
		TableName:        tableName,
		ColumnStatistics: make(map[string]ColumnStatisticsData),
	}

	reader := NewReader(data)
	if err := stats.UnmarshalBinary(reader); err != nil {
		return nil, err
	}

	return stats, nil
}

// DeleteStatisticsMetadata deletes statistics metadata for a table
func (m *MetadataManager) DeleteStatisticsMetadata(tableName string) error {
	// Remove from disk - statistics directory
	statsPath := filepath.Join(m.basePath, "statistics", tableName)
	if err := os.RemoveAll(statsPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	return nil
}
