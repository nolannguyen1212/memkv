package core

import (
	"sync"
)

type watchEntry struct {
	connID  uint64
	isDirty *bool
}

type WatchRegistry struct {
	mutex    sync.Mutex
	watchers map[string][]watchEntry
}

var globalWatch = &WatchRegistry{
	watchers: make(map[string][]watchEntry),
}

func (registry *WatchRegistry) Watch(key string, connID uint64, isDirty *bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.watchers[key] = append(registry.watchers[key], watchEntry{
		connID:  connID,
		isDirty: isDirty,
	})
}

func (registry *WatchRegistry) Notify(key string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, entry := range registry.watchers[key] {
		*entry.isDirty = true
	}
}

func UnwatchConn(connID uint64) {
	globalWatch.Unwatch(connID)
}

func (registry *WatchRegistry) Unwatch(connID uint64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	for key, entries := range registry.watchers {
		filtered := entries[:0]
		for _, entry := range entries {
			if entry.connID != connID {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			delete(registry.watchers, key)
		} else {
			registry.watchers[key] = filtered
		}
	}
}
