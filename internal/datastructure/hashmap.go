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

func (hashMap *HashMap) Set(key string, fieldValues ...string) int {
	hashMap.mutex.Lock()
	defer hashMap.mutex.Unlock()

	if len(fieldValues)%2 != 0 {
		return 0
	}
	if hashMap.entries[key] == nil {
		hashMap.entries[key] = make(map[string]string)
	}

	added := 0
	for i := 0; i < len(fieldValues); i += 2 {
		field, value := fieldValues[i], fieldValues[i+1]
		if _, exists := hashMap.entries[key][field]; !exists {
			added++
		}
		hashMap.entries[key][field] = value
	}
	return added
}

func (hashMap *HashMap) Get(key, field string) (string, bool) {
	hashMap.mutex.RLock()
	defer hashMap.mutex.RUnlock()

	hash, exists := hashMap.entries[key]
	if !exists {
		return "", false
	}
	value, exists := hash[field]
	return value, exists
}

func (hashMap *HashMap) Delete(key string, fields ...string) int {
	hashMap.mutex.Lock()
	defer hashMap.mutex.Unlock()

	hash, exists := hashMap.entries[key]
	if !exists {
		return 0
	}

	deleted := 0
	for _, field := range fields {
		if _, exists := hash[field]; exists {
			delete(hash, field)
			deleted++
		}
	}
	if len(hash) == 0 {
		delete(hashMap.entries, key)
	}
	return deleted
}

func (hashMap *HashMap) GetAll(key string) (map[string]string, bool) {
	hashMap.mutex.RLock()
	defer hashMap.mutex.RUnlock()

	hash, exists := hashMap.entries[key]
	if !exists {
		return nil, false
	}
	result := make(map[string]string, len(hash))
	maps.Copy(result, hash)
	return result, true
}

func (hashMap *HashMap) Exists(key, field string) bool {
	hashMap.mutex.RLock()
	defer hashMap.mutex.RUnlock()

	hash, exists := hashMap.entries[key]
	if !exists {
		return false
	}
	_, exists = hash[field]
	return exists
}

func (hashMap *HashMap) FieldCount(key string) int {
	hashMap.mutex.RLock()
	defer hashMap.mutex.RUnlock()

	return len(hashMap.entries[key])
}

func (hashMap *HashMap) Snapshot() map[string]map[string]string {
	hashMap.mutex.RLock()
	defer hashMap.mutex.RUnlock()

	snapshot := make(map[string]map[string]string, len(hashMap.entries))
	for key, hash := range hashMap.entries {
		hashCopy := make(map[string]string, len(hash))
		maps.Copy(hashCopy, hash)
		snapshot[key] = hashCopy
	}
	return snapshot
}
