package core

import (
	"log"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
)

type Store struct {
	Dictionary *datastructure.Dictionary
	Set        *datastructure.Set
	List       *datastructure.List
	Hash       *datastructure.Hash
	SortedSet  *datastructure.SortedSet
	PubSub     *datastructure.PubSub
	Eviction   *datastructure.EvictionManager
	// ExecMu is the ordering boundary between ordinary commands and EXEC.
	// Commands take a read lock, while EXEC holds the write lock for the whole
	// queued batch so no command from another connection can interleave with it.
	ExecMu sync.RWMutex
}

type StoreConfig struct {
	ExpirationCheckInterval time.Duration
	ExpirationMaxSampleSize int
	ExpirationMaxRounds     int
	KeyLimit                *int
	EvictStrategy           string
}

func NewStore(config StoreConfig) *Store {
	expConfig := datastructure.ExpirationConfig{
		CheckInterval:   config.ExpirationCheckInterval,
		MaxSampleSize:   config.ExpirationMaxSampleSize,
		MaxSampleRounds: config.ExpirationMaxRounds,
	}
	if expConfig.CheckInterval == 0 {
		expConfig = datastructure.DefaultExpirationConfig
	}

	store := &Store{
		Dictionary: datastructure.NewDictionary(expConfig),
		Set:        datastructure.NewSet(expConfig),
		List:       datastructure.NewList(),
		Hash:       datastructure.NewHash(),
		SortedSet:  datastructure.NewSortedSet(),
		PubSub:     datastructure.NewPubSub(),
	}

	store.Eviction = datastructure.NewEvictionManager(datastructure.EvictionConfig{
		Strategy: datastructure.EvictStrategy(config.EvictStrategy),
		KeyLimit: config.KeyLimit,
		OnEvict: func(key string) {
			store.deleteKey(key)
			log.Printf("Evicted key: %s", key)
		},
	})

	return store
}

func (store *Store) deleteKey(key string) {
	store.Dictionary.Delete(key)
	store.Set.Remove(key)
	store.List.LeftPop(key, store.List.Length(key))
	store.Hash.Delete(key)
	store.SortedSet.Remove(key)
}
