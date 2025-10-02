package datastructure

import (
	"strconv"
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

	deque := l.items[key]
	if deque == nil {
		return []Item{}
	}

	items := make([]Item, 0, count)
	for i := 0; i < count; i++ {
		item, ok := deque.PopFront()
		if !ok {
			break
		}
		items = append(items, item)
	}
	if deque.Empty() {
		delete(l.items, key)
	}
	return items
}

func (l *List) Rpop(key string, count int) []Item {
	l.mu.Lock()
	defer l.mu.Unlock()

	deque := l.items[key]
	if deque == nil {
		return []Item{}
	}

	items := make([]Item, 0, count)
	for i := 0; i < count; i++ {
		item, ok := deque.PopBack()
		if !ok {
			break
		}
		items = append(items, item)
	}
	if deque.Empty() {
		delete(l.items, key)
	}
	return items
}

func (l *List) Llen(key string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.items[key] == nil {
		return 0
	}

	return l.items[key].size
}

func (l *List) Lrange(key string, start int, stop int) ([]Item, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	deque := l.items[key]
	if deque == nil || deque.size == 0 {
		return []Item{}, false
	}

	if start < 0 {
		start += deque.size
	}
	if stop < 0 {
		stop += deque.size
	}
	if start < 0 {
		start = 0
	}
	if stop >= deque.size {
		stop = deque.size - 1
	}
	if start > stop {
		return []Item{}, true
	}

	items := make([]Item, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		pos := (deque.head + i) % deque.capacity
		items = append(items, deque.items[pos])
	}
	return items, true
}

func (l *List) Sort(key string, asc bool, alpha bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.items[key] == nil {
		return
	}

	l.items[key].Sort(func(i, j Item) bool {
		var less bool
		if alpha {
			less = i.Value < j.Value
		} else {
			a, erra := strconv.ParseFloat(i.Value, 64)
			b, errb := strconv.ParseFloat(j.Value, 64)
			if erra != nil || errb != nil {
				less = i.Value < j.Value
			} else {
				less = a < b
			}
		}
		if asc {
			return less
		}
		return !less
	})
}

func (l *List) Dump() map[string][]Item {
	l.mu.RLock()
	defer l.mu.RUnlock()

	snapshot := make(map[string][]Item, len(l.items))
	for key, deque := range l.items {
		if deque == nil || deque.size == 0 {
			continue
		}
		items := make([]Item, 0, deque.size)
		for i := 0; i < deque.size; i++ {
			pos := (deque.head + i) % deque.capacity
			items = append(items, deque.items[pos])
		}
		snapshot[key] = items
	}
	return snapshot
}
