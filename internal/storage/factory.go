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
