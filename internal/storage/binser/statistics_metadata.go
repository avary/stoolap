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
	"fmt"
	"time"
)

// ColumnStatisticsData holds serializable statistics for a column
type ColumnStatisticsData struct {
	// Column name
	ColumnName string

	// Count of total rows
	RowCount int64

	// Count of non-null values
	NonNullCount int64

	// Count of distinct values
	DistinctCount int64

	// Minimum value as string (may be empty)
	MinValue string

	// Maximum value as string (may be empty)
	MaxValue string

	// Last updated timestamp
	LastUpdated time.Time
}

// StatisticsMetadata holds serializable statistics for a table
type StatisticsMetadata struct {
	// Table name
	TableName string

	// Total row count
	RowCount int64

	// Last update timestamp
	LastUpdate time.Time

	// Last incremental update timestamp
	LastIncrementalUpdate time.Time

	// Statistics per column
	ColumnStatistics map[string]ColumnStatisticsData
}

// Type markers for statistics
const (
	TypeStatsMeta      byte = 20
	TypeColumnStatData byte = 21
)

// MarshalBinary marshals statistics metadata to binary
func (s *StatisticsMetadata) MarshalBinary(w *Writer) {
	// Write the type marker
	w.grow(1)
	w.buf = append(w.buf, TypeStatsMeta)

	// Write table name
	w.WriteString(s.TableName)

	// Write row count
	w.WriteInt64(s.RowCount)

	// Write timestamps
	w.WriteTime(s.LastUpdate)
	w.WriteTime(s.LastIncrementalUpdate)

	// Write column statistics
	// First, write column count
	w.WriteArrayHeader(len(s.ColumnStatistics))

	// Then each column statistics
	for colName, colStats := range s.ColumnStatistics {
		// Write column name
		w.WriteString(colName)

		// Write row count
		w.WriteInt64(colStats.RowCount)

		// Write non-null count
		w.WriteInt64(colStats.NonNullCount)

		// Write distinct count
		w.WriteInt64(colStats.DistinctCount)

		// Write min/max values
		w.WriteString(colStats.MinValue)
		w.WriteString(colStats.MaxValue)

		// Write last updated timestamp
		w.WriteTime(colStats.LastUpdated)
	}
}

// UnmarshalBinary unmarshals statistics metadata from binary
func (s *StatisticsMetadata) UnmarshalBinary(r *Reader) error {
	// Read type marker
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeStatsMeta {
		return fmt.Errorf("expected TypeStatsMeta (%d), got %d", TypeStatsMeta, tp)
	}

	var err error

	// Read table name
	s.TableName, err = r.ReadString()
	if err != nil {
		return fmt.Errorf("error reading table name: %w", err)
	}

	// Read row count
	s.RowCount, err = r.ReadInt64()
	if err != nil {
		return fmt.Errorf("error reading row count: %w", err)
	}

	// Read timestamps
	s.LastUpdate, err = r.ReadTime()
	if err != nil {
		return fmt.Errorf("error reading last update timestamp: %w", err)
	}

	s.LastIncrementalUpdate, err = r.ReadTime()
	if err != nil {
		return fmt.Errorf("error reading last incremental update timestamp: %w", err)
	}

	// Read column statistics
	// First, read the array header
	count, err := r.ReadArrayHeader()
	if err != nil {
		return fmt.Errorf("error reading column statistics array header: %w", err)
	}

	// Initialize the map
	s.ColumnStatistics = make(map[string]ColumnStatisticsData, count)

	// Read each column statistics
	for i := 0; i < count; i++ {
		// Read column name
		colName, err := r.ReadString()
		if err != nil {
			return fmt.Errorf("error reading column %d name: %w", i, err)
		}

		// Read statistics data
		colStats := ColumnStatisticsData{
			ColumnName: colName,
		}

		// Read row count
		colStats.RowCount, err = r.ReadInt64()
		if err != nil {
			return fmt.Errorf("error reading column %s row count: %w", colName, err)
		}

		// Read non-null count
		colStats.NonNullCount, err = r.ReadInt64()
		if err != nil {
			return fmt.Errorf("error reading column %s non-null count: %w", colName, err)
		}

		// Read distinct count
		colStats.DistinctCount, err = r.ReadInt64()
		if err != nil {
			return fmt.Errorf("error reading column %s distinct count: %w", colName, err)
		}

		// Read min/max values
		colStats.MinValue, err = r.ReadString()
		if err != nil {
			return fmt.Errorf("error reading column %s min value: %w", colName, err)
		}

		colStats.MaxValue, err = r.ReadString()
		if err != nil {
			return fmt.Errorf("error reading column %s max value: %w", colName, err)
		}

		// Read last updated timestamp
		colStats.LastUpdated, err = r.ReadTime()
		if err != nil {
			return fmt.Errorf("error reading column %s last updated timestamp: %w", colName, err)
		}

		// Add to the map
		s.ColumnStatistics[colName] = colStats
	}

	return nil
}
