package store

import "time"

const (
	TTLNotFound int64 = -2
	TTLNoExpire int64 = -1
)

type Status uint8

const (
	StatusMissing Status = iota
	StatusFound
	StatusWrongType
)

type KeyType string

const (
	KeyTypeString    KeyType = "string"
	KeyTypeSet       KeyType = "set"
	KeyTypeList      KeyType = "list"
	KeyTypeHash      KeyType = "hash"
	KeyTypeSortedSet KeyType = "zset"
)

type Entry struct {
	Type      KeyType
	Value     string
	Members   map[string]struct{}
	List      *Deque[string]
	Hash      map[string]string
	SortedSet *sortedSetEntry
	ExpiredAt time.Time
	Version   uint64
}

type StringEntry struct {
	Value string
}

type SetEntry struct {
	Members map[string]struct{}
}
