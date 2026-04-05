package core

import (
	"sync"
)

type watchEntry struct {
	connID uint64
	dirty  *bool
}

type WatchRegistry struct {
	mu       sync.Mutex
	watchers map[string][]watchEntry
}

var globalWatch = &WatchRegistry{
	watchers: make(map[string][]watchEntry),
}

func (r *WatchRegistry) Watch(key string, connID uint64, dirty *bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.watchers[key] = append(r.watchers[key], watchEntry{
		connID: connID,
		dirty:  dirty,
	})
}

func (r *WatchRegistry) Notify(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.watchers[key] {
		*entry.dirty = true
	}
}

func UnwatchConn(connID uint64) {
	globalWatch.Unwatch(connID)
}

func (r *WatchRegistry) Unwatch(connID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, entries := range r.watchers {
		filtered := entries[:0]
		for _, entry := range entries {
			if entry.connID != connID {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			delete(r.watchers, key)
		} else {
			r.watchers[key] = filtered
		}
	}
}
