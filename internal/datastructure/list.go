package datastructure

import (
	"sync"
)

type List struct {
	mu    sync.RWMutex
	items map[string]*Deque[Item]
}

func CreateList() *List {
	return &List{
		items: make(map[string]*Deque[Item]),
	}
}

func (l *List) Lpush(key string, values ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.items[key] == nil {
		l.items[key] = NewDeque[Item]()
	}

	for _, value := range values {
		item := Item{Value: value}
		l.items[key].PushFront(item)
	}
	return l.items[key].size
}

func (l *List) Rpush(key string, values ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.items[key] == nil {
		l.items[key] = NewDeque[Item]()
	}

	for _, value := range values {
		item := Item{Value: value}
		l.items[key].PushBack(item)
	}
	return l.items[key].size
}

func (l *List) Lpop(key string, count int) []Item {
	l.mu.Lock()
	defer l.mu.Unlock()

	items := make([]Item, 0, count)
	for range count {
		item, ok := l.items[key].PopFront()
		if ok {
			items = append(items, item)
		}
	}
	return items
}

func (l *List) Rpop(key string, count int) []Item {
	l.mu.Lock()
	defer l.mu.Unlock()

	items := make([]Item, 0, count)
	for range count {
		item, ok := l.items[key].PopBack()
		if ok {
			items = append(items, item)
		}
	}
	return items
}

func (l *List) Llen(key string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.items[key].size
}

func (l *List) Lrange(key string, start int, stop int) ([]Item, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	len := l.Llen(key)
	if start < 0 {
		start += len
	}

	if stop < 0 {
		stop += len
	}

	if stop >= len {
		stop = len - 1
	}

	if start >= stop {
		return []Item{}, true
	}

	items := make([]Item, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		pos := (l.items[key].head + i) % l.items[key].capacity
		items = append(items, l.items[key].items[pos])
	}

	return items, true
}
