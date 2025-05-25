/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package mvcc

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/stoolap/stoolap/internal/storage"
)

// MVCCFactory creates mvcc storage engines
type MVCCFactory struct{}

// Create implements the EngineFactory interface
func (f *MVCCFactory) Create(urlStr string) (storage.Engine, error) {
	// Handle different URL schemes for clarity
	var path string
	var persistenceEnabled bool
	var queryParams url.Values

	// Parse the URL for proper handling of paths and query parameters
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid URL format: %w", err)
	}

	// Extract query parameters
	queryParams = parsedURL.Query()

	switch parsedURL.Scheme {
	case "memory":
		// In-memory mode - no persistence
		persistenceEnabled = false
		path = ""

	case "file":
		// File mode - always persistent
		persistenceEnabled = true

		// Handle three specific scenarios:
		// 1. file:///var/folders/... - Absolute path (empty host, path starts with /)
		// 2. file://stoolap.db - Relative path in current folder (host is the filename)
		// 3. file://data/stoolap.db - Relative path with subfolder (host is the folder)

		// Case 1: Absolute path (file:///)
		if parsedURL.Host == "" && strings.HasPrefix(parsedURL.Path, "/") {
			// Absolute path - remove the leading slash
			path = "/" + parsedURL.Path[1:]

			// Special case for Windows paths like /C:/data/db.file
			if len(path) > 2 && path[1] == ':' && (path[2] == '/' || path[2] == '\\') {
				// Windows path with drive letter - keep it as is
			}
		} else if parsedURL.Host != "" {
			// If host is present, it's a relative path
			if parsedURL.Path == "" || parsedURL.Path == "/" {
				// Just the host (case 2)
				path = parsedURL.Host
			} else {
				// Host and path (case 3)
				// Remove the leading slash from path if present
				pathPart := parsedURL.Path
				pathPart = strings.TrimPrefix(pathPart, "/")
				path = filepath.Join(parsedURL.Host, pathPart)
			}
		} else {
			// Invalid format
			return nil, errors.New("file:// scheme requires a non-empty path")
		}

		// Final check to ensure we have a path
		if path == "" {
			return nil, errors.New("file:// scheme requires a non-empty path")
		}

	default:
		return nil, errors.New("unsupported scheme: must use 'memory://' or 'file://'")
	}

	// Process query parameters for configuration

	// Configure the engine with default values
	config := &storage.Config{
		Path:        path,
		Persistence: storage.DefaultPersistenceConfig(),
	}

	// Set persistence based on the URL scheme - this is non-negotiable based on scheme
	config.Persistence.Enabled = persistenceEnabled

	// Parse sync mode
	if queryParams != nil {
		if syncMode := queryParams.Get("sync_mode"); syncMode != "" {
			switch syncMode {
			case "none":
				config.Persistence.SyncMode = 0
			case "normal":
				config.Persistence.SyncMode = 1
			case "full":
				config.Persistence.SyncMode = 2
			default:
				// Try to parse as int
				if mode, err := strconv.Atoi(syncMode); err == nil && mode >= 0 && mode <= 2 {
					config.Persistence.SyncMode = mode
				}
			}
		}
	}

	// Parse other configuration parameters if we have query parameters
	if queryParams != nil {
		// Parse snapshot interval
		if interval := queryParams.Get("snapshot_interval"); interval != "" {
			if val, err := strconv.Atoi(interval); err == nil && val > 0 {
				config.Persistence.SnapshotInterval = val
			}
		}

		// Parse keep snapshots
		if keep := queryParams.Get("keep_snapshots"); keep != "" {
			if val, err := strconv.Atoi(keep); err == nil && val > 0 {
				config.Persistence.KeepSnapshots = val
			}
		}

		// Parse WAL flush trigger
		if trigger := queryParams.Get("wal_flush_trigger"); trigger != "" {
			if val, err := strconv.Atoi(trigger); err == nil && val > 0 {
				config.Persistence.WALFlushTrigger = val
			}
		}

		// Parse WAL buffer size
		if bufSize := queryParams.Get("wal_buffer_size"); bufSize != "" {
			if val, err := strconv.Atoi(bufSize); err == nil && val > 0 {
				config.Persistence.WALBufferSize = val
			}
		}
	}

	// Create and return the engine
	engine := NewMVCCEngine(config)
	return engine, nil
}

func init() {
	// Register the MVCC factory with the storage engine registry
	storage.RegisterEngineFactory("mvcc", &MVCCFactory{})
}
