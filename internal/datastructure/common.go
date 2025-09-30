package datastructure

import (
	"time"

	"github.com/william1nguyen/valkeydb/internal/config"
)

type Item struct {
	Value     string
	Members   map[string]struct{}
	ExpiredAt time.Time
}

func GetMaxSampleSize() int {
	if config.Global != nil {
		return config.Global.Datastructure.Expiration.MaxSampleSize
	}
	return 20
}

func GetMaxSampleRounds() int {
	if config.Global != nil {
		return config.Global.Datastructure.Expiration.MaxSampleRounds
	}
	return 3
}

func GetExpirationCheckInterval() time.Duration {
	if config.Global != nil {
		return config.Global.GetExpirationCheckInterval()
	}
	return time.Second
}
