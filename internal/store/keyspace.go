package store

import (
	"container/heap"
	"time"
)

type KeyMetadata struct {
	Type      KeyType
	Version   uint64
	ExpiredAt time.Time
}

type Keyspace struct {
	store    *Store
	onExpire func(string)
}

func NewKeyspace(store *Store, onExpire func(string)) *Keyspace {
	return &Keyspace{store: store, onExpire: onExpire}
}

func (keyspace *Keyspace) Type(key string) (KeyType, bool) {
	keyspace.expireIfNeeded(key)
	entry, exists := keyspace.store.entries[key]

	if !exists {
		return "", false
	}

	return entry.Type, true
}

func (keyspace *Keyspace) BumpVersion(key string) uint64 {
	keyspace.store.versions[key]++

	if entry := keyspace.store.entries[key]; entry != nil {
		entry.Version = keyspace.store.versions[key]
	}

	if keyspace.store.mutationHandler != nil {
		keyspace.store.mutationHandler(key)
	}

	return keyspace.store.versions[key]
}

func (keyspace *Keyspace) Version(key string) uint64 {
	keyspace.expireIfNeeded(key)
	return keyspace.store.versions[key]
}

func (keyspace *Keyspace) Delete(key string) bool {
	if _, exists := keyspace.store.entries[key]; !exists {
		return false
	}

	delete(keyspace.store.entries, key)
	keyspace.BumpVersion(key)
	return true
}

func (keyspace *Keyspace) RemoveIfType(key string, keyType KeyType) bool {
	entry := keyspace.store.entries[key]

	if entry == nil || entry.Type != keyType {
		return false
	}

	delete(keyspace.store.entries, key)
	return true
}

func (keyspace *Keyspace) ExpireAt(key string, at time.Time) bool {
	entry := keyspace.store.entries[key]

	if entry == nil {
		return false
	}

	keyspace.BumpVersion(key)

	if !at.After(keyspace.store.Now()) {
		delete(keyspace.store.entries, key)
		keyspace.expired(key)
		return true
	}

	entry.ExpiredAt = at
	keyspace.store.scheduleExpiration(key, entry)
	return true
}

func (keyspace *Keyspace) TTL(key string) int64 {
	keyspace.expireIfNeeded(key)
	entry := keyspace.store.entries[key]

	if entry == nil {
		return TTLNotFound
	}

	if entry.ExpiredAt.IsZero() {
		return TTLNoExpire
	}

	return int64(entry.ExpiredAt.Sub(keyspace.store.Now()).Seconds())
}

func (keyspace *Keyspace) Snapshot() map[string]KeyMetadata {
	keyspace.expireAll(keyspace.store.Now())
	snapshot := make(map[string]KeyMetadata, len(keyspace.store.entries))

	for key, entry := range keyspace.store.entries {
		snapshot[key] = KeyMetadata{
			Type:      entry.Type,
			Version:   entry.Version,
			ExpiredAt: entry.ExpiredAt,
		}
	}

	return snapshot
}

func (keyspace *Keyspace) Restore(key string, metadata KeyMetadata) {
	if !metadata.ExpiredAt.IsZero() && !metadata.ExpiredAt.After(keyspace.store.Now()) {
		return
	}

	keyspace.store.entries[key] = &Entry{
		Type:      metadata.Type,
		Version:   metadata.Version,
		ExpiredAt: metadata.ExpiredAt,
	}

	keyspace.store.versions[key] = metadata.Version
}

func (keyspace *Keyspace) expireIfNeeded(key string) {
	entry := keyspace.store.entries[key]

	if entry == nil {
		return
	}

	if entry.ExpiredAt.IsZero() {
		return
	}

	if entry.ExpiredAt.After(keyspace.store.Now()) {
		return
	}

	delete(keyspace.store.entries, key)
	keyspace.BumpVersion(key)
	keyspace.expired(key)
}

func (keyspace *Keyspace) expireAll(now time.Time) {
	for key, entry := range keyspace.store.entries {
		if !entry.ExpiredAt.IsZero() && !entry.ExpiredAt.After(now) {
			delete(keyspace.store.entries, key)
			keyspace.BumpVersion(key)
			keyspace.expired(key)
		}
	}
}

func (keyspace *Keyspace) expireDue(now time.Time, limit int) {
	for range limit {
		if len(keyspace.store.expirations) == 0 {
			return
		}

		if keyspace.store.expirations[0].at > now.UnixMilli() {
			return
		}

		item := heap.Pop(&keyspace.store.expirations).(expirationItem)
		entry := keyspace.store.entries[item.key]

		if entry == nil {
			continue
		}

		if entry.Version != item.version {
			continue
		}

		if entry.ExpiredAt.UnixMilli() != item.at {
			continue
		}

		delete(keyspace.store.entries, item.key)
		keyspace.BumpVersion(item.key)
		keyspace.expired(item.key)
	}
}

func (keyspace *Keyspace) expired(key string) {
	if keyspace.onExpire != nil {
		keyspace.onExpire(key)
	}

	if keyspace.store.expirationHandler != nil {
		keyspace.store.expirationHandler(key)
	}
}
