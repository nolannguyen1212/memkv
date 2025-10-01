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

	if l.items[key] == nil {
		return []Item{}
	}

	items := make([]Item, 0, count)
	for range count {
		deque, exist := l.items[key]
		if exist {
			item, ok := deque.PopFront()
			if ok {
				items = append(items, item)
			}
		}
	}
	return items
}

func (l *List) Rpop(key string, count int) []Item {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.items[key] == nil {
		return []Item{}
	}

	items := make([]Item, 0, count)
	for range count {
		deque, exist := l.items[key]
		if exist {
			item, ok := deque.PopBack()
			if ok {
				items = append(items, item)
			}
		}
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

	if l.items[key] == nil {
		return []Item{}, false
	}

	len := l.items[key].size
	if len == 0 {
		return []Item{}, false
	}

	if start < 0 {
		start += len
	}

	if stop < 0 {
		stop += len
	}

	if stop >= len {
		stop = len - 1
	}

	if start > stop {
		return []Item{}, true
	}

	items := make([]Item, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		pos := (l.items[key].head + i) % l.items[key].capacity
		items = append(items, l.items[key].items[pos])
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
