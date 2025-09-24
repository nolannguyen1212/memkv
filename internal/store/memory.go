package store

import (
	"sync"
	"time"
)

type entry struct {
	value     string
	expiredAt time.Time
}

type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]entry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records: make(map[string]entry),
	}
}

func (m *MemoryStore) Set(key, value string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := entry{value: value}

	if ttl > 0 {
		e.expiredAt = time.Now().Add(ttl)
	}

	m.records[key] = e
}

func (m *MemoryStore) Get(key string) (string, bool) {
	m.mu.RLock()
	e, ok := m.records[key]
	m.mu.RUnlock()

	if !ok {
		return "", false
	}

	if !e.expiredAt.IsZero() && time.Now().After(e.expiredAt) {
		m.mu.Lock()
		delete(m.records, key)
		m.mu.Unlock()
		return "", false
	}

	return e.value, true
}

func (m *MemoryStore) Delete(keys ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	removed := 0
	for _, k := range keys {
		if _, ok := m.records[k]; ok {
			delete(m.records, k)
			removed++
		}
	}
	return removed
}

func (m *MemoryStore) Expire(key string, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.records[key]
	if !ok {
		return false
	}

	if ttl <= 0 {
		delete(m.records, key)
		return true
	}

	e.expiredAt = time.Now().Add(ttl)
	m.records[key] = e

	return true
}

func (m *MemoryStore) TTL(key string) int64 {
	m.mu.RLock()
	e, ok := m.records[key]
	m.mu.RUnlock()

	if !ok {
		return -2
	}

	if e.expiredAt.IsZero() {
		return -1
	}

	remaining := time.Until(e.expiredAt).Seconds()
	if remaining < 0 {
		m.mu.Lock()
		delete(m.records, key)
		m.mu.Unlock()
		return -2
	}

	return int64(remaining)
}
