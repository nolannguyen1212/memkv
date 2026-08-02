package store

import (
	"fmt"
	"time"
)

const defaultExpirationInterval = time.Second
const expirationWorkLimit = 128

type Store struct {
	Keyspace                *Keyspace
	Dictionary              *Dictionary
	Set                     *Set
	List                    *List
	Hash                    *Hash
	SortedSet               *SortedSet
	Eviction                *EvictionManager
	entries                 map[string]*Entry
	versions                map[string]uint64
	expirationCheckInterval time.Duration
	nextExpirationCheck     time.Time
	keyLimit                *int
	evictStrategy           string
	expirations             expirationHeap
	clock                   Clock
	expirationHandler       func(string)
}

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

type Config struct {
	ExpirationCheckInterval time.Duration
	KeyLimit                *int
	EvictStrategy           string
	Clock                   Clock
}

type LogicalSnapshot struct {
	Keyspace   map[string]KeyMetadata
	Versions   map[string]uint64
	Strings    map[string]StringEntry
	Sets       map[string]SetEntry
	Lists      map[string][]string
	Hashes     map[string]map[string]string
	SortedSets map[string]map[string]float64
}

func New(config Config) *Store {
	interval := config.ExpirationCheckInterval
	if interval <= 0 {
		interval = defaultExpirationInterval
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	database := &Store{
		entries:                 make(map[string]*Entry),
		versions:                make(map[string]uint64),
		expirationCheckInterval: interval,
		nextExpirationCheck:     clock.Now().Add(interval),
		keyLimit:                config.KeyLimit,
		evictStrategy:           config.EvictStrategy,
		clock:                   clock,
	}
	database.Dictionary = &Dictionary{store: database}
	database.Set = &Set{store: database}
	database.List = &List{store: database}
	database.Hash = &Hash{store: database}
	database.SortedSet = &SortedSet{store: database}
	database.Keyspace = NewKeyspace(database, func(key string) {
		if database.Eviction != nil {
			database.Eviction.RecordDelete(key)
		}
	})
	database.Eviction = NewEvictionManager(EvictionConfig{
		Strategy: EvictStrategy(config.EvictStrategy),
		KeyLimit: config.KeyLimit,
	})
	return database
}

func (store *Store) Now() time.Time {
	return store.clock.Now()
}

func (store *Store) lookupEntry(key string) *Entry {
	store.Keyspace.expireIfNeeded(key)
	return store.entries[key]
}

func (store *Store) SetExpirationHandler(handler func(string)) {
	store.expirationHandler = handler
}

func (store *Store) PrepareWrite(key string, keyType KeyType, replace bool) bool {
	_, ok := store.Keyspace.Claim(key, keyType, replace)
	return ok
}

func (store *Store) CheckType(key string, expected KeyType) Status {
	actual, exists := store.Keyspace.Type(key)
	if !exists {
		return StatusMissing
	}
	if actual != expected {
		return StatusWrongType
	}
	return StatusFound
}

func (store *Store) SetString(key, value string, expiredAt time.Time) {
	store.entries[key] = &Entry{
		Type:      KeyTypeString,
		Value:     value,
		ExpiredAt: expiredAt,
		Version:   store.versions[key],
	}
	store.Keyspace.BumpVersion(key)
	store.scheduleExpiration(key, store.entries[key])
	store.Eviction.RecordInsert(key)
}

func (store *Store) DeleteKey(key string) bool {
	if !store.Keyspace.Delete(key) {
		return false
	}
	store.Eviction.RecordDelete(key)
	return true
}

func (store *Store) RemoveEmptyKey(key string, keyType KeyType) {
	if store.Keyspace.RemoveIfType(key, keyType) {
		store.Eviction.RecordDelete(key)
	}
}

func (store *Store) Clear() []string {
	metadata := store.Keyspace.Snapshot()
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		if store.DeleteKey(key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (store *Store) Snapshot() LogicalSnapshot {
	return LogicalSnapshot{
		Keyspace:   store.Keyspace.Snapshot(),
		Versions:   cloneVersions(store.versions),
		Strings:    store.Dictionary.Snapshot(),
		Sets:       store.Set.Snapshot(),
		Lists:      store.List.Snapshot(),
		Hashes:     store.Hash.Snapshot(),
		SortedSets: store.SortedSet.Snapshot(),
	}
}

func (store *Store) Maintain(now time.Time) {
	if now.Before(store.nextExpirationCheck) {
		return
	}
	store.Keyspace.expireDue(now, expirationWorkLimit)
	store.nextExpirationCheck = now.Add(store.expirationCheckInterval)
}

func (store *Store) ExpirationCheckInterval() time.Duration {
	return store.expirationCheckInterval
}

func (store *Store) Restore(state LogicalSnapshot, now time.Time) error {
	entries, versions, err := restoredEntries(state, now)
	if err != nil {
		return err
	}
	store.entries = entries
	store.versions = versions
	store.expirations = nil
	for key, entry := range entries {
		store.scheduleExpiration(key, entry)
	}
	store.nextExpirationCheck = now.Add(store.expirationCheckInterval)
	store.Eviction.Reset()
	for key := range entries {
		store.Eviction.RecordInsert(key)
	}
	return nil
}

func (store *Store) Clone(now time.Time) (*Store, error) {
	clone := New(Config{
		ExpirationCheckInterval: store.expirationCheckInterval,
		KeyLimit:                store.keyLimit,
		EvictStrategy:           store.evictStrategy,
		Clock:                   store.clock,
	})
	if err := clone.Restore(store.Snapshot(), now); err != nil {
		return nil, err
	}
	return clone, nil
}

func restoredEntries(state LogicalSnapshot, now time.Time) (map[string]*Entry, map[string]uint64, error) {
	entries := make(map[string]*Entry, len(state.Keyspace))
	versions := cloneVersions(state.Versions)
	for key, metadata := range state.Keyspace {
		if version, exists := versions[key]; exists && version != metadata.Version {
			return nil, nil, fmt.Errorf("version mismatch for key %q", key)
		}
		versions[key] = metadata.Version
		if !metadata.ExpiredAt.IsZero() && !metadata.ExpiredAt.After(now) {
			continue
		}
		entry := &Entry{
			Type:      metadata.Type,
			Version:   metadata.Version,
			ExpiredAt: metadata.ExpiredAt,
		}
		switch metadata.Type {
		case KeyTypeString:
			value, exists := state.Strings[key]
			if !exists {
				return nil, nil, fmt.Errorf("string payload missing for key %q", key)
			}
			entry.Value = value.Value
		case KeyTypeSet:
			value, exists := state.Sets[key]
			if !exists || len(value.Members) == 0 {
				return nil, nil, fmt.Errorf("set payload missing or empty for key %q", key)
			}
			entry.Members = cloneSet(value.Members)
		case KeyTypeList:
			value, exists := state.Lists[key]
			if !exists || len(value) == 0 {
				return nil, nil, fmt.Errorf("list payload missing or empty for key %q", key)
			}
			entry.List = NewDeque[string]()
			for _, item := range value {
				entry.List.PushBack(item)
			}
		case KeyTypeHash:
			value, exists := state.Hashes[key]
			if !exists || len(value) == 0 {
				return nil, nil, fmt.Errorf("hash payload missing or empty for key %q", key)
			}
			entry.Hash = cloneHash(value)
		case KeyTypeSortedSet:
			value, exists := state.SortedSets[key]
			if !exists || len(value) == 0 {
				return nil, nil, fmt.Errorf("sorted set payload missing or empty for key %q", key)
			}
			entry.SortedSet = &sortedSetEntry{scores: cloneScores(value)}
			entry.SortedSet.rebuild()
		default:
			return nil, nil, fmt.Errorf("unsupported key type %q for key %q", metadata.Type, key)
		}
		entries[key] = entry
	}
	if err := validatePayloadKeys(state); err != nil {
		return nil, nil, err
	}
	return entries, versions, nil
}

func validatePayloadKeys(state LogicalSnapshot) error {
	for key := range state.Strings {
		if metadata, exists := state.Keyspace[key]; !exists || metadata.Type != KeyTypeString {
			return fmt.Errorf("string payload has no matching metadata for key %q", key)
		}
	}
	for key := range state.Sets {
		if metadata, exists := state.Keyspace[key]; !exists || metadata.Type != KeyTypeSet {
			return fmt.Errorf("set payload has no matching metadata for key %q", key)
		}
	}
	for key := range state.Lists {
		if metadata, exists := state.Keyspace[key]; !exists || metadata.Type != KeyTypeList {
			return fmt.Errorf("list payload has no matching metadata for key %q", key)
		}
	}
	for key := range state.Hashes {
		if metadata, exists := state.Keyspace[key]; !exists || metadata.Type != KeyTypeHash {
			return fmt.Errorf("hash payload has no matching metadata for key %q", key)
		}
	}
	for key := range state.SortedSets {
		if metadata, exists := state.Keyspace[key]; !exists || metadata.Type != KeyTypeSortedSet {
			return fmt.Errorf("sorted set payload has no matching metadata for key %q", key)
		}
	}
	return nil
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	clone := make(map[string]struct{}, len(source))
	for member := range source {
		clone[member] = struct{}{}
	}
	return clone
}

func cloneHash(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for field, value := range source {
		clone[field] = value
	}
	return clone
}

func cloneScores(source map[string]float64) map[string]float64 {
	clone := make(map[string]float64, len(source))
	for member, score := range source {
		clone[member] = score
	}
	return clone
}

func cloneVersions(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, version := range source {
		clone[key] = version
	}
	return clone
}
