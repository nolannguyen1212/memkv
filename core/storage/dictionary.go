package storage

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

func NewDictionary(expirationConfig ExpirationConfig) *Dictionary {
	dictionary := &Dictionary{
		entries:          make(map[string]Entry),
		expirationConfig: expirationConfig,
	}
	go dictionary.runExpirationLoop()
	return dictionary
}

func (dictionary *Dictionary) Set(key, value string, timeToLive time.Duration) {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry := Entry{Value: value}
	if timeToLive > 0 {
		entry.ExpiredAt = time.Now().Add(timeToLive)
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

	deletedCount := 0
	for _, key := range keys {
		if _, exists := dictionary.entries[key]; exists {
			delete(dictionary.entries, key)
			deletedCount++
		}
	}
	return deletedCount
}

func (dictionary *Dictionary) Expire(key string, timeToLive time.Duration) bool {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry, exists := dictionary.entries[key]
	if !exists {
		return false
	}

	if timeToLive <= 0 {
		delete(dictionary.entries, key)
		return true
	}

	entry.ExpiredAt = time.Now().Add(timeToLive)
	dictionary.entries[key] = entry
	return true
}

func (dictionary *Dictionary) ExpireAt(key string, expireTime time.Time) bool {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	entry, exists := dictionary.entries[key]
	if !exists {
		return false
	}

	if expireTime.Before(time.Now()) {
		delete(dictionary.entries, key)
		return true
	}

	entry.ExpiredAt = expireTime
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

	remainingSeconds := time.Until(entry.ExpiredAt).Seconds()
	if remainingSeconds < 0 {
		dictionary.delete(key)
		return TTLNotFound
	}

	return int64(remainingSeconds)
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
		dictionary.performActiveExpiration()
	}
}

func (dictionary *Dictionary) performActiveExpiration() {
	dictionary.mutex.Lock()
	defer dictionary.mutex.Unlock()

	if len(dictionary.entries) == 0 {
		return
	}

	for round := 0; round < dictionary.expirationConfig.MaxSampleRounds; round++ {
		checkedCount := 0
		expiredCount := 0

		keys := dictionary.collectKeys()
		sampleSize := min(dictionary.expirationConfig.MaxSampleSize, len(keys))

		for sample := 0; sample < sampleSize; sample++ {
			randomIndex := rand.IntN(len(keys))
			key := keys[randomIndex]

			if entry, exists := dictionary.entries[key]; exists {
				checkedCount++
				if entry.IsExpired() {
					delete(dictionary.entries, key)
					expiredCount++
				}
			}
		}

		if checkedCount == 0 || expiredCount*ExpiryRateStop < checkedCount {
			return
		}
	}
}

func (dictionary *Dictionary) collectKeys() []string {
	keys := make([]string, 0, len(dictionary.entries))
	for key := range dictionary.entries {
		keys = append(keys, key)
	}
	return keys
}
