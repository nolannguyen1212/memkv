package store

import (
	"math/rand/v2"
	"sync"
	"time"
)

type entry struct {
	value     string
	expiredAt time.Time
}

type MemoryStore struct {
	mu      sync.RWMutex
	ttlKeys map[string]struct{}
	records map[string]entry
	quit    chan struct{}
}

func NewMemoryStore() *MemoryStore {
	m := &MemoryStore{
		ttlKeys: make(map[string]struct{}),
		records: make(map[string]entry),
		quit:    make(chan struct{}),
	}
	go m.expireLoop()
	return m
}

func (m *MemoryStore) Close() {
	close(m.quit)
}

func (m *MemoryStore) Set(key, value string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := entry{value: value}

	if ttl > 0 {
		e.expiredAt = time.Now().Add(ttl)
		m.addTTLKey(key)
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
		if e, ok := m.records[key]; ok && !e.expiredAt.IsZero() && time.Now().After(e.expiredAt) {
			delete(m.records, key)
			m.removeTTLKey(key)
		}
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
			m.removeTTLKey(k)
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
		if e, ok := m.records[key]; ok && !e.expiredAt.IsZero() && time.Until(e.expiredAt).Seconds() < 0 {
			delete(m.records, key)
			m.removeTTLKey(key)
		}
		m.mu.Unlock()
		return -2
	}

	return int64(remaining)
}

func (m *MemoryStore) expireLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			m.sampleExpire(20)
			m.mu.Unlock()
		case <-m.quit:
			return
		}
	}
}

func (m *MemoryStore) sampleExpire(batchSize int) {
	if len(m.ttlKeys) == 0 {
		return
	}

	now := time.Now()
	maxCycles := 3

	for range maxCycles {
		if len(m.ttlKeys) == 0 {
			return
		}

		checked := 0
		expired := 0

		keys := make([]string, 0, len(m.ttlKeys))
		for k := range m.ttlKeys {
			keys = append(keys, k)
		}

		sampleSize := batchSize
		if sampleSize > len(keys) {
			sampleSize = len(keys)
		}

		for i := 0; i < sampleSize; i++ {
			idx := rand.IntN(len(keys))
			key := keys[idx]

			e, ok := m.records[key]
			if !ok {
				m.removeTTLKey(key)
				continue
			}

			checked++
			if !e.expiredAt.IsZero() && now.After(e.expiredAt) {
				delete(m.records, key)
				m.removeTTLKey(key)
				expired++
			}
		}

		if checked == 0 || expired*4 < checked {
			return
		}
	}
}

func (m *MemoryStore) addTTLKey(k string) {
	m.ttlKeys[k] = struct{}{}
}

func (m *MemoryStore) removeTTLKey(k string) {
	delete(m.ttlKeys, k)
}
