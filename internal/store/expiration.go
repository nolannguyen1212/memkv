package store

import "container/heap"

type expirationItem struct {
	key     string
	at      int64
	version uint64
}

type expirationHeap []expirationItem

func (items expirationHeap) Len() int {
	return len(items)
}

func (items expirationHeap) Less(i, j int) bool {
	if items[i].at == items[j].at {
		return items[i].key < items[j].key
	}

	return items[i].at < items[j].at
}

func (items expirationHeap) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func (items *expirationHeap) Push(value any) {
	*items = append(*items, value.(expirationItem))
}

func (items *expirationHeap) Pop() any {
	old := *items
	last := len(old) - 1
	value := old[last]
	*items = old[:last]
	return value
}

func (store *Store) scheduleExpiration(key string, entry *Entry) {
	if entry.ExpiredAt.IsZero() {
		return
	}

	heap.Push(&store.expirations, expirationItem{
		key:     key,
		at:      entry.ExpiredAt.UnixMilli(),
		version: entry.Version,
	})
}
