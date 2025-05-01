package storage

import (
	"sync"
)

// EngineFactory is the interface for factories that create storage engines
type EngineFactory interface {
	Create(url string) (Engine, error)
}

// Registry of engine factories
var (
	engineFactories = make(map[string]EngineFactory)
	factoryMutex    sync.RWMutex
)

// RegisterEngineFactory registers a storage engine factory
func RegisterEngineFactory(name string, factory EngineFactory) {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	engineFactories[name] = factory
}

// GetEngineFactory returns a storage engine factory
func GetEngineFactory(name string) EngineFactory {
	factoryMutex.RLock()
	defer factoryMutex.RUnlock()
	return engineFactories[name]
}
