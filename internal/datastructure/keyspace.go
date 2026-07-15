package datastructure

import (
	"sync"
	"time"
)

type KeyType string

const (
	KeyTypeString    KeyType = "string"
	KeyTypeSet       KeyType = "set"
	KeyTypeList      KeyType = "list"
	KeyTypeHash      KeyType = "hash"
	KeyTypeSortedSet KeyType = "zset"
)

type KeyMetadata struct {
	Type      KeyType
	Version   uint64
	ExpiredAt time.Time
}

// Keyspace is the authoritative catalog for key type, version, and expiry.
// Concrete data structures only own the payload associated with a key.
type Keyspace struct {
	mutex        sync.RWMutex
	entries      map[string]KeyMetadata
	versions     map[string]uint64
	onExpire     func(string)
	interval     time.Duration
	runExclusive func(func())
}

func NewKeyspace(interval time.Duration, onExpire func(string), runExclusive func(func())) *Keyspace {
	if interval <= 0 {
		interval = DefaultExpirationConfig.CheckInterval
	}
	keyspace := &Keyspace{
		entries:      make(map[string]KeyMetadata),
		versions:     make(map[string]uint64),
		onExpire:     onExpire,
		interval:     interval,
		runExclusive: runExclusive,
	}
	go keyspace.runExpirationLoop()
	return keyspace
}

func (keyspace *Keyspace) Claim(key string, keyType KeyType, replace bool) (KeyType, bool) {
	keyspace.expireIfNeeded(key)

	keyspace.mutex.Lock()
	defer keyspace.mutex.Unlock()
	entry, exists := keyspace.entries[key]
	if exists && entry.Type != keyType && !replace {
		return entry.Type, false
	}
	if !exists {
		keyspace.entries[key] = KeyMetadata{Type: keyType, Version: keyspace.versions[key]}
		return "", true
	}
	if entry.Type == keyType {
		if replace {
			entry.ExpiredAt = time.Time{}
			keyspace.entries[key] = entry
		}
		return "", true
	}
	oldType := entry.Type
	entry.Type = keyType
	entry.ExpiredAt = time.Time{}
	keyspace.entries[key] = entry
	return oldType, true
}

func (keyspace *Keyspace) Type(key string) (KeyType, bool) {
	keyspace.expireIfNeeded(key)
	keyspace.mutex.RLock()
	defer keyspace.mutex.RUnlock()
	entry, exists := keyspace.entries[key]
	return entry.Type, exists
}

func (keyspace *Keyspace) BumpVersion(key string) uint64 {
	keyspace.mutex.Lock()
	defer keyspace.mutex.Unlock()
	keyspace.versions[key]++
	if entry, exists := keyspace.entries[key]; exists {
		entry.Version = keyspace.versions[key]
		keyspace.entries[key] = entry
	}
	return keyspace.versions[key]
}

func (keyspace *Keyspace) Version(key string) uint64 {
	keyspace.expireIfNeeded(key)
	keyspace.mutex.RLock()
	defer keyspace.mutex.RUnlock()
	return keyspace.versions[key]
}

func (keyspace *Keyspace) Delete(key string) bool {
	keyspace.mutex.Lock()
	_, exists := keyspace.entries[key]
	if exists {
		delete(keyspace.entries, key)
		keyspace.versions[key]++
	}
	keyspace.mutex.Unlock()
	return exists
}

func (keyspace *Keyspace) RemoveIfType(key string, keyType KeyType) bool {
	keyspace.mutex.Lock()
	defer keyspace.mutex.Unlock()
	entry, exists := keyspace.entries[key]
	if !exists || entry.Type != keyType {
		return false
	}
	delete(keyspace.entries, key)
	return true
}

func (keyspace *Keyspace) Expire(key string, ttl time.Duration) bool {
	return keyspace.ExpireAt(key, time.Now().Add(ttl))
}

func (keyspace *Keyspace) ExpireAt(key string, at time.Time) bool {
	keyspace.mutex.Lock()
	entry, exists := keyspace.entries[key]
	if !exists {
		keyspace.mutex.Unlock()
		return false
	}
	if !at.After(time.Now()) {
		delete(keyspace.entries, key)
		keyspace.versions[key]++
		keyspace.mutex.Unlock()
		keyspace.expirePayload(key)
		return true
	}
	entry.ExpiredAt = at
	keyspace.entries[key] = entry
	keyspace.mutex.Unlock()
	return true
}

func (keyspace *Keyspace) TTL(key string) int64 {
	keyspace.expireIfNeeded(key)
	keyspace.mutex.RLock()
	defer keyspace.mutex.RUnlock()
	entry, exists := keyspace.entries[key]
	if !exists {
		return TTLNotFound
	}
	if entry.ExpiredAt.IsZero() {
		return TTLNoExpire
	}
	return int64(time.Until(entry.ExpiredAt).Seconds())
}

func (keyspace *Keyspace) Snapshot() map[string]KeyMetadata {
	keyspace.expireAll()
	keyspace.mutex.RLock()
	defer keyspace.mutex.RUnlock()
	result := make(map[string]KeyMetadata, len(keyspace.entries))
	for key, entry := range keyspace.entries {
		result[key] = entry
	}
	return result
}

func (keyspace *Keyspace) Restore(key string, metadata KeyMetadata) {
	if !metadata.ExpiredAt.IsZero() && !metadata.ExpiredAt.After(time.Now()) {
		return
	}
	keyspace.mutex.Lock()
	defer keyspace.mutex.Unlock()
	keyspace.entries[key] = metadata
	if metadata.Version > keyspace.versions[key] {
		keyspace.versions[key] = metadata.Version
	}
}

func (keyspace *Keyspace) expireIfNeeded(key string) {
	keyspace.mutex.Lock()
	entry, exists := keyspace.entries[key]
	if !exists || entry.ExpiredAt.IsZero() || entry.ExpiredAt.After(time.Now()) {
		keyspace.mutex.Unlock()
		return
	}
	delete(keyspace.entries, key)
	keyspace.versions[key]++
	keyspace.mutex.Unlock()
	keyspace.expirePayload(key)
}

func (keyspace *Keyspace) expireAll() {
	now := time.Now()
	var expired []string
	keyspace.mutex.Lock()
	for key, entry := range keyspace.entries {
		if !entry.ExpiredAt.IsZero() && !entry.ExpiredAt.After(now) {
			delete(keyspace.entries, key)
			keyspace.versions[key]++
			expired = append(expired, key)
		}
	}
	keyspace.mutex.Unlock()
	for _, key := range expired {
		keyspace.expirePayload(key)
	}
}

func (keyspace *Keyspace) expirePayload(key string) {
	if keyspace.onExpire != nil {
		keyspace.onExpire(key)
	}
}

func (keyspace *Keyspace) runExpirationLoop() {
	ticker := time.NewTicker(keyspace.interval)
	defer ticker.Stop()
	for range ticker.C {
		if keyspace.runExclusive == nil {
			keyspace.expireAll()
			continue
		}
		keyspace.runExclusive(keyspace.expireAll)
	}
}
