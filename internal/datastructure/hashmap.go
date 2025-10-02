package datastructure

import "sync"

type HashMap struct {
	mu    sync.RWMutex
	items map[string]map[string]string
}

func CreateHashMap() *HashMap {
	return &HashMap{
		items: make(map[string]map[string]string),
	}
}

func (h *HashMap) Hset(key string, fieldValues ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(fieldValues)%2 != 0 {
		return 0
	}

	if h.items[key] == nil {
		h.items[key] = make(map[string]string)
	}

	added := 0
	for i := 0; i < len(fieldValues); i += 2 {
		field := fieldValues[i]
		value := fieldValues[i+1]
		_, exists := h.items[key][field]
		h.items[key][field] = value
		if !exists {
			added++
		}
	}
	return added
}

func (h *HashMap) Hget(key, field string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, ok := h.items[key]
	if !ok {
		return "", false
	}
	val, ok := hash[field]
	return val, ok
}

func (h *HashMap) Hdel(key string, fields ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	hash, ok := h.items[key]
	if !ok {
		return 0
	}

	count := 0
	for _, field := range fields {
		if _, exists := hash[field]; exists {
			delete(hash, field)
			count++
		}
	}

	if len(hash) == 0 {
		delete(h.items, key)
	}
	return count
}

func (h *HashMap) Hgetall(key string) (map[string]string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, ok := h.items[key]
	if !ok {
		return nil, false
	}

	result := make(map[string]string, len(hash))
	for k, v := range hash {
		result[k] = v
	}
	return result, true
}

func (h *HashMap) Hexists(key, field string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, ok := h.items[key]
	if !ok {
		return false
	}
	_, exists := hash[field]
	return exists
}

func (h *HashMap) Hlen(key string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, ok := h.items[key]
	if !ok {
		return 0
	}
	return len(hash)
}

func (h *HashMap) Dump() map[string]map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snapshot := make(map[string]map[string]string, len(h.items))
	for key, hash := range h.items {
		hashCopy := make(map[string]string, len(hash))
		for field, value := range hash {
			hashCopy[field] = value
		}
		snapshot[key] = hashCopy
	}
	return snapshot
}
