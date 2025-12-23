package datastructure

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
	dict := &Dictionary{
		entries:          make(map[string]Entry),
		expirationConfig: config,
	}
	go dict.runExpirationLoop()
	return dict
}

func (dict *Dictionary) Set(key, value string, ttl time.Duration) {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	entry := Entry{Value: value}
	if ttl > 0 {
		entry.ExpiredAt = time.Now().Add(ttl)
	}
	dict.entries[key] = entry
}

func (dict *Dictionary) Get(key string) (string, bool) {
	dict.mutex.RLock()
	entry, exists := dict.entries[key]
	dict.mutex.RUnlock()

	if !exists {
		return "", false
	}
	if entry.IsExpired() {
		dict.delete(key)
		return "", false
	}
	return entry.Value, true
}

func (dict *Dictionary) Delete(keys ...string) int {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	count := 0
	for _, key := range keys {
		if _, exists := dict.entries[key]; exists {
			delete(dict.entries, key)
			count++
		}
	}
	return count
}

func (dict *Dictionary) Expire(key string, ttl time.Duration) bool {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	entry, exists := dict.entries[key]
	if !exists {
		return false
	}
	if ttl <= 0 {
		delete(dict.entries, key)
		return true
	}
	entry.ExpiredAt = time.Now().Add(ttl)
	dict.entries[key] = entry
	return true
}

func (dict *Dictionary) ExpireAt(key string, at time.Time) bool {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	entry, exists := dict.entries[key]
	if !exists {
		return false
	}
	if at.Before(time.Now()) {
		delete(dict.entries, key)
		return true
	}
	entry.ExpiredAt = at
	dict.entries[key] = entry
	return true
}

func (dict *Dictionary) TTL(key string) int64 {
	dict.mutex.RLock()
	entry, exists := dict.entries[key]
	dict.mutex.RUnlock()

	if !exists {
		return TTLNotFound
	}
	if entry.ExpiredAt.IsZero() {
		return TTLNoExpire
	}
	remaining := time.Until(entry.ExpiredAt).Seconds()
	if remaining < 0 {
		dict.delete(key)
		return TTLNotFound
	}
	return int64(remaining)
}

func (dict *Dictionary) Snapshot() map[string]Entry {
	dict.mutex.RLock()
	defer dict.mutex.RUnlock()

	snapshot := make(map[string]Entry, len(dict.entries))
	for key, entry := range dict.entries {
		if !entry.IsExpired() {
			snapshot[key] = entry
		}
	}
	return snapshot
}

func (dict *Dictionary) delete(key string) {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()
	delete(dict.entries, key)
}

func (dict *Dictionary) runExpirationLoop() {
	ticker := time.NewTicker(dict.expirationConfig.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		dict.expireKeys()
	}
}

func (dict *Dictionary) expireKeys() {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	if len(dict.entries) == 0 {
		return
	}

	keys := make([]string, 0, len(dict.entries))
	for k := range dict.entries {
		keys = append(keys, k)
	}

	for range dict.expirationConfig.MaxSampleRounds {
		checked, expired := 0, 0
		sampleSize := min(dict.expirationConfig.MaxSampleSize, len(keys))

		for i := 0; i < sampleSize; i++ {
			key := keys[rand.IntN(len(keys))]
			if entry, exists := dict.entries[key]; exists {
				checked++
				if entry.IsExpired() {
					delete(dict.entries, key)
					expired++
				}
			}
		}

		if checked == 0 || expired*4 < checked {
			return
		}
	}
}
