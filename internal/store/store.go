package store

import "time"

type Entry struct {
	Value     string
	ExpiredAt time.Time
}

type Store interface {
	Set(key, value string, ttl time.Duration)
	Get(key string) (string, bool)
	Delete(keys ...string) int
	Expire(key string, ttl time.Duration) bool
	ExpireAt(key string, at time.Time) bool
	TTL(key string) int64
	Dump() map[string]Entry
}
