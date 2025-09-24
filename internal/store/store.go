package store

import "time"

type Store interface {
	Set(key, value string, ttl time.Duration)
	Get(key string) (string, bool)
	Delete(keys ...string) int
	Expire(key string, ttl time.Duration) bool
	TTL(key string) int64
}
