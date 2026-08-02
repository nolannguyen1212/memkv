package engine

type watchEntry struct {
	connID uint64
}

type watchRegistry struct {
	watchers map[string][]watchEntry
	dirty    map[uint64]bool
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{
		watchers: make(map[string][]watchEntry),
		dirty:    make(map[uint64]bool),
	}
}

func (registry *watchRegistry) watch(key string, connID uint64) {
	for _, entry := range registry.watchers[key] {
		if entry.connID == connID {
			return
		}
	}

	registry.watchers[key] = append(registry.watchers[key], watchEntry{connID: connID})
}

func (registry *watchRegistry) notify(key string) {
	for _, entry := range registry.watchers[key] {
		registry.dirty[entry.connID] = true
	}
}

func (registry *watchRegistry) isDirty(connID uint64) bool {
	return registry.dirty[connID]
}

func (registry *watchRegistry) unwatch(connID uint64) {
	delete(registry.dirty, connID)

	for key, entries := range registry.watchers {
		remaining := entries[:0]

		for _, entry := range entries {
			if entry.connID != connID {
				remaining = append(remaining, entry)
			}
		}

		if len(remaining) == 0 {
			delete(registry.watchers, key)
			continue
		}

		registry.watchers[key] = remaining
	}
}
