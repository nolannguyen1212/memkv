package store

import (
	"time"

	"github.com/william1nguyen/valkeydb/core/storage"
)

type Store struct {
	Dictionary *storage.Dictionary
	Set        *storage.Set
	List       *storage.List
	HashMap    *storage.HashMap
	PubSub     *storage.PubSub
}

type Config struct {
	ExpirationCheckInterval time.Duration
	ExpirationMaxSampleSize int
	ExpirationMaxRounds     int
}

func New(config Config) *Store {
	expirationConfig := storage.ExpirationConfig{
		CheckInterval:   config.ExpirationCheckInterval,
		MaxSampleSize:   config.ExpirationMaxSampleSize,
		MaxSampleRounds: config.ExpirationMaxRounds,
	}

	if expirationConfig.CheckInterval == 0 {
		expirationConfig = storage.DefaultExpirationConfig
	}

	return &Store{
		Dictionary: storage.NewDictionary(expirationConfig),
		Set:        storage.NewSet(expirationConfig),
		List:       storage.NewList(),
		HashMap:    storage.NewHashMap(),
		PubSub:     storage.NewPubSub(),
	}
}
