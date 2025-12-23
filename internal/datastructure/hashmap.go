package datastructure

import (
	"maps"
	"sync"
)

type HashMap struct {
	mutex   sync.RWMutex
	entries map[string]map[string]string
}

func NewHashMap() *HashMap {
	return &HashMap{entries: make(map[string]map[string]string)}
}

func (h *HashMap) Set(key string, fieldValues ...string) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if len(fieldValues)%2 != 0 {
		return 0
	}
	if h.entries[key] == nil {
		h.entries[key] = make(map[string]string)
	}

	added := 0
	for i := 0; i < len(fieldValues); i += 2 {
		field, value := fieldValues[i], fieldValues[i+1]
		if _, exists := h.entries[key][field]; !exists {
			added++
		}
		h.entries[key][field] = value
	}
	return added
}

func (h *HashMap) Get(key, field string) (string, bool) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	hash, exists := h.entries[key]
	if !exists {
		return "", false
	}
	value, exists := hash[field]
	return value, exists
}

func (h *HashMap) Delete(key string, fields ...string) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	hash, exists := h.entries[key]
	if !exists {
		return 0
	}

	deleted := 0
	for _, f := range fields {
		if _, exists := hash[f]; exists {
			delete(hash, f)
			deleted++
		}
	}
	if len(hash) == 0 {
		delete(h.entries, key)
	}
	return deleted
}

func (h *HashMap) GetAll(key string) (map[string]string, bool) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	hash, exists := h.entries[key]
	if !exists {
		return nil, false
	}
	result := make(map[string]string, len(hash))
	maps.Copy(result, hash)
	return result, true
}

func (h *HashMap) Exists(key, field string) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	hash, exists := h.entries[key]
	if !exists {
		return false
	}
	_, exists = hash[field]
	return exists
}

func (h *HashMap) FieldCount(key string) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return len(h.entries[key])
}

func (h *HashMap) Snapshot() map[string]map[string]string {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	snapshot := make(map[string]map[string]string, len(h.entries))
	for key, hash := range h.entries {
		hashCopy := make(map[string]string, len(hash))
		maps.Copy(hashCopy, hash)
		snapshot[key] = hashCopy
	}
	return snapshot
}
