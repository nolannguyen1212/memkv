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
	HashMap    *datastructure.HashMap
	SortedList *datastructure.SortedList
	PubSub     *datastructure.PubSub
	Eviction   *datastructure.EvictionManager
	ExecMu     sync.Mutex
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
		HashMap:    datastructure.NewHashMap(),
		SortedList: datastructure.NewSortedList(),
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

func (s *Store) deleteKey(key string) {
	s.Dictionary.Delete(key)
	s.Set.Remove(key)
	s.List.LeftPop(key, s.List.Length(key))
	s.HashMap.Delete(key)
	s.SortedList.Remove(key)
}
