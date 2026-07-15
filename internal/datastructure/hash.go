package datastructure

import (
	"maps"
	"sync"
)

type Hash struct {
	mutex   sync.RWMutex
	entries map[string]map[string]string
}

func NewHash() *Hash {
	return &Hash{entries: make(map[string]map[string]string)}
}

func (hash *Hash) Set(key string, fieldValues ...string) int {
	hash.mutex.Lock()
	defer hash.mutex.Unlock()

	if len(fieldValues)%2 != 0 {
		return 0
	}
	if hash.entries[key] == nil {
		hash.entries[key] = make(map[string]string)
	}

	added := 0
	for i := 0; i < len(fieldValues); i += 2 {
		field, value := fieldValues[i], fieldValues[i+1]
		if _, exists := hash.entries[key][field]; !exists {
			added++
		}
		hash.entries[key][field] = value
	}
	return added
}

func (hash *Hash) Get(key, field string) (string, bool) {
	hash.mutex.RLock()
	defer hash.mutex.RUnlock()

	fields, exists := hash.entries[key]
	if !exists {
		return "", false
	}
	value, exists := fields[field]
	return value, exists
}

func (hash *Hash) Delete(key string, fields ...string) int {
	hash.mutex.Lock()
	defer hash.mutex.Unlock()

	existing, exists := hash.entries[key]
	if !exists {
		return 0
	}

	deleted := 0
	for _, field := range fields {
		if _, exists := existing[field]; exists {
			delete(existing, field)
			deleted++
		}
	}
	if len(existing) == 0 {
		delete(hash.entries, key)
	}
	return deleted
}

func (hash *Hash) DeleteKey(key string) bool {
	hash.mutex.Lock()
	defer hash.mutex.Unlock()
	if _, exists := hash.entries[key]; !exists {
		return false
	}
	delete(hash.entries, key)
	return true
}

func (hash *Hash) GetAll(key string) (map[string]string, bool) {
	hash.mutex.RLock()
	defer hash.mutex.RUnlock()

	fields, exists := hash.entries[key]
	if !exists {
		return nil, false
	}
	result := make(map[string]string, len(fields))
	maps.Copy(result, fields)
	return result, true
}

func (hash *Hash) Exists(key, field string) bool {
	hash.mutex.RLock()
	defer hash.mutex.RUnlock()

	fields, exists := hash.entries[key]
	if !exists {
		return false
	}
	_, exists = fields[field]
	return exists
}

func (hash *Hash) FieldCount(key string) int {
	hash.mutex.RLock()
	defer hash.mutex.RUnlock()
	return len(hash.entries[key])
}

func (hash *Hash) Snapshot() map[string]map[string]string {
	hash.mutex.RLock()
	defer hash.mutex.RUnlock()

	snapshot := make(map[string]map[string]string, len(hash.entries))
	for key, fields := range hash.entries {
		fieldsCopy := make(map[string]string, len(fields))
		maps.Copy(fieldsCopy, fields)
		snapshot[key] = fieldsCopy
	}
	return snapshot
}
