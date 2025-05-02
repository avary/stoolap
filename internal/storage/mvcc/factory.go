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

		// Handle path extraction from URL
		// For URLs like file://db/anotherfolder, we need to combine host and path
		path = parsedURL.Path

		// Path validation and normalization
		if path == "" || path == "/" {
			// Extract the host part if it exists - this covers file://db style URLs
			if parsedURL.Host != "" {
				path = parsedURL.Host
			} else {
				return nil, errors.New("file:// scheme requires a non-empty path")
			}
		} else {
			// For URLs like file://db/folder where db is the host and /folder is the path
			if parsedURL.Host != "" {
				// Combine host and path, removing the leading slash from path
				if strings.HasPrefix(path, "/") {
					path = filepath.Join(parsedURL.Host, path[1:])
				} else {
					path = filepath.Join(parsedURL.Host, path)
				}
			} else if strings.HasPrefix(path, "/") {
				// Handle normal absolute paths like file:///absolute/path
				// Just remove the leading slash
				path = path[1:]

				// Special case for Windows paths like /C:/data/db.file
				if len(path) > 2 && path[1] == ':' && (path[2] == '/' || path[2] == '\\') {
					// Windows path with drive letter - keep it as is
				}
			}
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
