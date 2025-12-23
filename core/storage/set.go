package storage

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

func NewSet(expirationConfig ExpirationConfig) *Set {
	set := &Set{
		entries:          make(map[string]SetEntry),
		expirationConfig: expirationConfig,
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

	addedCount := 0
	for _, member := range members {
		if _, memberExists := entry.Members[member]; !memberExists {
			entry.Members[member] = struct{}{}
			addedCount++
		}
	}

	set.entries[key] = entry
	return addedCount
}

func (set *Set) Remove(key string, members ...string) int {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists || len(entry.Members) == 0 {
		return 0
	}

	removedCount := 0
	for _, member := range members {
		if _, memberExists := entry.Members[member]; memberExists {
			delete(entry.Members, member)
			removedCount++
		}
	}

	if len(entry.Members) == 0 {
		delete(set.entries, key)
	} else {
		set.entries[key] = entry
	}

	return removedCount
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

	if !exists {
		return false
	}

	if entry.IsExpired() {
		set.delete(key)
		return false
	}

	_, memberExists := entry.Members[member]
	return memberExists
}

func (set *Set) Cardinality(key string) int {
	set.mutex.RLock()
	entry, exists := set.entries[key]
	set.mutex.RUnlock()

	if !exists {
		return 0
	}

	if entry.IsExpired() {
		set.delete(key)
		return 0
	}

	return len(entry.Members)
}

func (set *Set) Expire(key string, timeToLive time.Duration) bool {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		return false
	}

	if timeToLive <= 0 {
		delete(set.entries, key)
		return true
	}

	entry.ExpiredAt = time.Now().Add(timeToLive)
	set.entries[key] = entry
	return true
}

func (set *Set) ExpireAt(key string, expireTime time.Time) bool {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	entry, exists := set.entries[key]
	if !exists {
		return false
	}

	if expireTime.Before(time.Now()) {
		delete(set.entries, key)
		return true
	}

	entry.ExpiredAt = expireTime
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

	remainingSeconds := time.Until(entry.ExpiredAt).Seconds()
	if remainingSeconds < 0 {
		set.delete(key)
		return TTLNotFound
	}

	return int64(remainingSeconds)
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
		set.performActiveExpiration()
	}
}

func (set *Set) performActiveExpiration() {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	if len(set.entries) == 0 {
		return
	}

	for range set.expirationConfig.MaxSampleRounds {
		checkedCount, expiredCount := 0, 0

		keys := make([]string, 0, len(set.entries))
		for key := range set.entries {
			keys = append(keys, key)
		}

		if len(keys) == 0 {
			return
		}

		sampleSize := min(set.expirationConfig.MaxSampleSize, len(keys))

		for sample := 0; sample < sampleSize; sample++ {
			randomIndex := rand.IntN(len(keys))
			key := keys[randomIndex]

			if entry, exists := set.entries[key]; exists {
				checkedCount++
				if entry.IsExpired() {
					delete(set.entries, key)
					expiredCount++
				}
			}
		}

		if checkedCount == 0 || expiredCount*ExpiryRateStop < checkedCount {
			return
		}
	}
}
