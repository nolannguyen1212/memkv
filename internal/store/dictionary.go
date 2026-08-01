package store

import (
	"math/rand/v2"
	"sync"
	"time"
)

type ExpirationConfig struct {
	CheckInterval   time.Duration
	MaxSampleSize   int
	MaxSampleRounds int
}

var DefaultExpirationConfig = ExpirationConfig{
	CheckInterval:   time.Second,
	MaxSampleSize:   20,
	MaxSampleRounds: 3,
}

type Dictionary struct {
	mutex            sync.RWMutex
	entries          map[string]Entry
	expirationConfig ExpirationConfig
}

func NewDictionary(config ExpirationConfig) *Dictionary {
	if config.CheckInterval == 0 {
		config = DefaultExpirationConfig
	}
	dictionary := &Dictionary{
		entries:          make(map[string]Entry),
		expirationConfig: config,
	}
	go dictionary.runExpirationLoop()
	return dictionary
}

func (dictionary *Dictionary) Set(key, value string, ttl time.Duration) {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry := Entry{Value: value}
	if prev, exists := dictionary.entries[key]; exists {
		entry.Version = prev.Version + 1
	}
	if ttl > 0 {
		entry.ExpiredAt = time.Now().Add(ttl)
	}
	dictionary.entries[key] = entry
}

func (dictionary *Dictionary) Get(key string) (string, bool) {
	dictionary.mutex.RLock()
	entry, exists := dictionary.entries[key]
	dictionary.mutex.RUnlock()

	if !exists {
		return "", false
	}
	if entry.IsExpired() {
		dictionary.delete(key)
		return "", false
	}
	return entry.Value, true
}

func (dictionary *Dictionary) Delete(keys ...string) int {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	count := 0
	for _, key := range keys {
		if _, exists := dictionary.entries[key]; exists {
			delete(dictionary.entries, key)
			count++
		}
	}
	return count
}

func (dictionary *Dictionary) Expire(key string, ttl time.Duration) bool {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry, exists := dictionary.entries[key]
	if !exists {
		return false
	}
	if ttl <= 0 {
		delete(dictionary.entries, key)
		return true
	}
	entry.ExpiredAt = time.Now().Add(ttl)
	dictionary.entries[key] = entry
	return true
}

func (dictionary *Dictionary) ExpireAt(key string, at time.Time) bool {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry, exists := dictionary.entries[key]
	if !exists {
		return false
	}
	if at.Before(time.Now()) {
		delete(dictionary.entries, key)
		return true
	}
	entry.ExpiredAt = at
	dictionary.entries[key] = entry
	return true
}

func (dictionary *Dictionary) TTL(key string) int64 {
	dictionary.mutex.RLock()
	entry, exists := dictionary.entries[key]
	dictionary.mutex.RUnlock()

	if !exists {
		return TTLNotFound
	}
	if entry.ExpiredAt.IsZero() {
		return TTLNoExpire
	}
	remaining := time.Until(entry.ExpiredAt).Seconds()
	if remaining < 0 {
		dictionary.delete(key)
		return TTLNotFound
	}
	return int64(remaining)
}

func (dictionary *Dictionary) Snapshot() map[string]Entry {
	dictionary.mutex.RLock()
	defer dictionary.mutex.RUnlock()

	snapshot := make(map[string]Entry, len(dictionary.entries))
	for key, entry := range dictionary.entries {
		if !entry.IsExpired() {
			snapshot[key] = entry
		}
	}
	return snapshot
}

func (dictionary *Dictionary) delete(key string) {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()
	delete(dictionary.entries, key)
}

func (dictionary *Dictionary) runExpirationLoop() {
	ticker := time.NewTicker(dictionary.expirationConfig.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		dictionary.expireKeys()
	}
}

func (dictionary *Dictionary) expireKeys() {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	if len(dictionary.entries) == 0 {
		return
	}

	keys := make([]string, 0, len(dictionary.entries))
	for key := range dictionary.entries {
		keys = append(keys, key)
	}

	for range dictionary.expirationConfig.MaxSampleRounds {
		checked, expired := 0, 0
		sampleSize := min(dictionary.expirationConfig.MaxSampleSize, len(keys))

		for i := 0; i < sampleSize; i++ {
			key := keys[rand.IntN(len(keys))]
			if entry, exists := dictionary.entries[key]; exists {
				checked++
				if entry.IsExpired() {
					delete(dictionary.entries, key)
					expired++
				}
			}
		}

		if checked == 0 || expired*4 < checked {
			return
		}
	}
}

func (dictionary *Dictionary) Version(key string) uint64 {
	dictionary.mutex.RLock()
	defer dictionary.mutex.RUnlock()
	return dictionary.entries[key].Version
}
