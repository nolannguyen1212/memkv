package store

import "time"

const (
	TTLNotFound int64 = -2
	TTLNoExpire int64 = -1
)

type Entry struct {
	Value     string
	ExpiredAt time.Time
	Version   uint64
}

type SetEntry struct {
	Members   map[string]struct{}
	ExpiredAt time.Time
}

func (entry Entry) IsExpired() bool {
	return !entry.ExpiredAt.IsZero() && time.Now().After(entry.ExpiredAt)
}

func (entry SetEntry) IsExpired() bool {
	return !entry.ExpiredAt.IsZero() && time.Now().After(entry.ExpiredAt)
}
