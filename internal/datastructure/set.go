package datastructure

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Set struct {
	mutex            sync.RWMutex
	entries          map[string]SetEntry
	expirationConfig ExpirationConfig
}

func NewSet(config ExpirationConfig) *Set {
	if config.CheckInterval == 0 {
		config = DefaultExpirationConfig
	}
	set := &Set{
		entries:          make(map[string]SetEntry),
		expirationConfig: config,
	}
	go set.runExpirationLoop()
	return set
}

func (set *Set) Add(key string, members ...string) int {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		entry = SetEntry{Members: make(map[string]struct{})}
	}

	added := 0
	for _, member := range members {
		if _, ok := entry.Members[member]; !ok {
			entry.Members[member] = struct{}{}
			added++
		}
	}
	set.entries[key] = entry
	return added
}

func (set *Set) Remove(key string, members ...string) int {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		return 0
	}

	removed := 0
	for _, member := range members {
		if _, ok := entry.Members[member]; ok {
			delete(entry.Members, member)
			removed++
		}
	}
	if len(entry.Members) == 0 {
		delete(set.entries, key)
	} else {
		set.entries[key] = entry
	}
	return removed
}

func (set *Set) Members(key string) ([]string, bool) {
	set.mutex.RLock()
	entry, exists := set.entries[key]
	set.mutex.RUnlock()

	if !exists {
		return nil, false
	}
	if entry.IsExpired() {
		set.delete(key)
		return nil, false
	}

	members := make([]string, 0, len(entry.Members))
	for member := range entry.Members {
		members = append(members, member)
	}
	return members, true
}

func (set *Set) IsMember(key, member string) bool {
	set.mutex.RLock()
	entry, exists := set.entries[key]
	set.mutex.RUnlock()

	if !exists || entry.IsExpired() {
		return false
	}
	_, ok := entry.Members[member]
	return ok
}

func (set *Set) Cardinality(key string) int {
	set.mutex.RLock()
	entry, exists := set.entries[key]
	set.mutex.RUnlock()

	if !exists || entry.IsExpired() {
		return 0
	}
	return len(entry.Members)
}

func (set *Set) Expire(key string, ttl time.Duration) bool {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		return false
	}
	if ttl <= 0 {
		delete(set.entries, key)
		return true
	}
	entry.ExpiredAt = time.Now().Add(ttl)
	set.entries[key] = entry
	return true
}

func (set *Set) ExpireAt(key string, at time.Time) bool {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		return false
	}
	entry.ExpiredAt = at
	set.entries[key] = entry
	return true
}

func (set *Set) TTL(key string) int64 {
	set.mutex.RLock()
	entry, exists := set.entries[key]
	set.mutex.RUnlock()

	if !exists {
		return TTLNotFound
	}
	if entry.ExpiredAt.IsZero() {
		return TTLNoExpire
	}
	remaining := time.Until(entry.ExpiredAt).Seconds()
	if remaining < 0 {
		set.delete(key)
		return TTLNotFound
	}
	return int64(remaining)
}

func (set *Set) Snapshot() map[string]SetEntry {
	set.mutex.RLock()
	defer set.mutex.RUnlock()

	snapshot := make(map[string]SetEntry, len(set.entries))
	for key, entry := range set.entries {
		if entry.IsExpired() {
			continue
		}
		membersCopy := make(map[string]struct{}, len(entry.Members))
		for member := range entry.Members {
			membersCopy[member] = struct{}{}
		}
		snapshot[key] = SetEntry{Members: membersCopy, ExpiredAt: entry.ExpiredAt}
	}
	return snapshot
}

func (set *Set) delete(key string) {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	delete(set.entries, key)
}

func (set *Set) runExpirationLoop() {
	ticker := time.NewTicker(set.expirationConfig.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		set.expireKeys()
	}
}

func (set *Set) expireKeys() {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	if len(set.entries) == 0 {
		return
	}

	keys := make([]string, 0, len(set.entries))
	for k := range set.entries {
		keys = append(keys, k)
	}

	for range set.expirationConfig.MaxSampleRounds {
		checked, expired := 0, 0
		sampleSize := min(set.expirationConfig.MaxSampleSize, len(keys))

		for i := 0; i < sampleSize; i++ {
			key := keys[rand.IntN(len(keys))]
			if entry, exists := set.entries[key]; exists {
				checked++
				if entry.IsExpired() {
					delete(set.entries, key)
					expired++
				}
			}
		}

		if checked == 0 || expired*4 < checked {
			return
		}
	}
}
