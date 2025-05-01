package mvcc

import (
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/semihalev/stoolap/internal/storage"
)

// MVCCFactory creates mvcc storage engines
type MVCCFactory struct{}

// Create implements the EngineFactory interface
func (f *MVCCFactory) Create(urlStr string) (storage.Engine, error) {
	// Parse the URL
	uri, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	// Check that the scheme is db
	if uri.Scheme != "db" {
		return nil, errors.New("unsupported scheme: " + uri.Scheme)
	}

	// Extract the path
	path := uri.Path
	if path == "" {
		return nil, errors.New("empty path")
	}

	path = strings.TrimPrefix(path, "/")

	// Parse query parameters for configuration
	query := uri.Query()

	// Configure the engine with default values
	config := &storage.Config{
		Path:        path,
		Persistence: storage.DefaultPersistenceConfig(),
	}

	// Parse persistence options from URL parameters
	if enabled := query.Get("persistence"); enabled != "" {
		config.Persistence.Enabled = enabled == "true" || enabled == "1" || enabled == "yes"
	}

	// Parse sync mode
	if syncMode := query.Get("sync_mode"); syncMode != "" {
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

	// Parse snapshot interval
	if interval := query.Get("snapshot_interval"); interval != "" {
		if val, err := strconv.Atoi(interval); err == nil && val > 0 {
			config.Persistence.SnapshotInterval = val
		}
	}

	// Parse keep snapshots
	if keep := query.Get("keep_snapshots"); keep != "" {
		if val, err := strconv.Atoi(keep); err == nil && val > 0 {
			config.Persistence.KeepSnapshots = val
		}
	}

	// Parse WAL flush trigger
	if trigger := query.Get("wal_flush_trigger"); trigger != "" {
		if val, err := strconv.Atoi(trigger); err == nil && val > 0 {
			config.Persistence.WALFlushTrigger = val
		}
	}

	// Parse WAL buffer size
	if bufSize := query.Get("wal_buffer_size"); bufSize != "" {
		if val, err := strconv.Atoi(bufSize); err == nil && val > 0 {
			config.Persistence.WALBufferSize = val
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
