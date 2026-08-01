package store

import (
	"log"
	"sync"
	"time"
)

type Store struct {
	Keyspace   *Keyspace
	Dictionary *Dictionary
	Set        *Set
	List       *List
	Hash       *Hash
	SortedSet  *SortedSet
	PubSub     *PubSub
	Eviction   *EvictionManager
	// ExecMu is the ordering boundary between ordinary commands and EXEC.
	// Commands take a read lock, while EXEC holds the write lock for the whole
	// queued batch so no command from another connection can interleave with it.
	ExecMu sync.RWMutex
}

type Config struct {
	ExpirationCheckInterval time.Duration
	ExpirationMaxSampleSize int
	ExpirationMaxRounds     int
	KeyLimit                *int
	EvictStrategy           string
}

func New(config Config) *Store {
	expConfig := ExpirationConfig{
		CheckInterval:   config.ExpirationCheckInterval,
		MaxSampleSize:   config.ExpirationMaxSampleSize,
		MaxSampleRounds: config.ExpirationMaxRounds,
	}
	if expConfig.CheckInterval == 0 {
		expConfig = DefaultExpirationConfig
	}

	store := &Store{
		Dictionary: NewDictionary(expConfig),
		Set:        NewSet(expConfig),
		List:       NewList(),
		Hash:       NewHash(),
		SortedSet:  NewSortedSet(),
		PubSub:     NewPubSub(),
	}
	store.Keyspace = NewKeyspace(
		expConfig.CheckInterval,
		func(key string) {
			store.deleteValue(key)
			if store.Eviction != nil {
				store.Eviction.RecordDelete(key)
			}
		},
		func(expire func()) {
			store.ExecMu.Lock()
			defer store.ExecMu.Unlock()
			expire()
		},
	)

	store.Eviction = NewEvictionManager(EvictionConfig{
		Strategy: EvictStrategy(config.EvictStrategy),
		KeyLimit: config.KeyLimit,
		OnEvict: func(key string) {
			store.Keyspace.Delete(key)
			store.deleteValue(key)
			log.Printf("Evicted key: %s", key)
		},
	})

	return store
}

func (store *Store) PrepareWrite(key string, keyType KeyType, replace bool) bool {
	oldType, ok := store.Keyspace.Claim(key, keyType, replace)
	if !ok {
		return false
	}
	if oldType != "" {
		store.deleteValue(key)
	}
	return true
}

func (store *Store) HasType(key string, expected KeyType) (exists bool, valid bool) {
	actual, exists := store.Keyspace.Type(key)
	return exists, !exists || actual == expected
}

func (store *Store) DeleteKey(key string) bool {
	if !store.Keyspace.Delete(key) {
		return false
	}
	store.deleteValue(key)
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

func (store *Store) deleteValue(key string) {
	store.Dictionary.Delete(key)
	store.Set.DeleteKey(key)
	store.List.DeleteKey(key)
	store.Hash.DeleteKey(key)
	store.SortedSet.DeleteKey(key)
}
