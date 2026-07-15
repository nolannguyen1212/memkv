package datastructure

import (
	"strconv"
	"sync"
)

type List struct {
	mutex   sync.RWMutex
	entries map[string]*Deque[string]
}

func NewList() *List {
	return &List{entries: make(map[string]*Deque[string])}
}

func (list *List) LeftPush(key string, values ...string) int {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.entries[key] == nil {
		list.entries[key] = NewDeque[string]()
	}
	for _, v := range values {
		list.entries[key].PushFront(v)
	}
	return list.entries[key].Length()
}

func (list *List) RightPush(key string, values ...string) int {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.entries[key] == nil {
		list.entries[key] = NewDeque[string]()
	}
	for _, v := range values {
		list.entries[key].PushBack(v)
	}
	return list.entries[key].Length()
}

func (list *List) LeftPop(key string, count int) []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	d := list.entries[key]
	if d == nil {
		return []string{}
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		v, ok := d.PopFront()
		if !ok {
			break
		}
		result = append(result, v)
	}
	if d.IsEmpty() {
		delete(list.entries, key)
	}
	return result
}

func (list *List) RightPop(key string, count int) []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	d := list.entries[key]
	if d == nil {
		return []string{}
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		v, ok := d.PopBack()
		if !ok {
			break
		}
		result = append(result, v)
	}
	if d.IsEmpty() {
		delete(list.entries, key)
	}
	return result
}

func (list *List) DeleteKey(key string) bool {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	if _, exists := list.entries[key]; !exists {
		return false
	}
	delete(list.entries, key)
	return true
}

func (list *List) Length(key string) int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	if list.entries[key] == nil {
		return 0
	}
	return list.entries[key].Length()
}

func (list *List) Range(key string, start, stop int) ([]string, bool) {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	d := list.entries[key]
	if d == nil || d.Length() == 0 {
		return []string{}, false
	}

	length := d.Length()
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}, true
	}

	result := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		result = append(result, d.At(i))
	}
	return result, true
}

func (list *List) Sort(key string, ascending, alpha bool) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	d := list.entries[key]
	if d == nil {
		return
	}

	d.Sort(func(a, b string) bool {
		less := compareStrings(a, b, alpha)
		if ascending {
			return less
		}
		return !less
	})
}

func (list *List) Snapshot() map[string][]string {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	snapshot := make(map[string][]string, len(list.entries))
	for key, d := range list.entries {
		if d == nil || d.Length() == 0 {
			continue
		}
		values := make([]string, d.Length())
		for i := 0; i < d.Length(); i++ {
			values[i] = d.At(i)
		}
		snapshot[key] = values
	}
	return snapshot
}

func compareStrings(a, b string, alpha bool) bool {
	if alpha {
		return a < b
	}
	aNum, aErr := strconv.ParseFloat(a, 64)
	bNum, bErr := strconv.ParseFloat(b, 64)
	if aErr != nil || bErr != nil {
		return a < b
	}
	return aNum < bNum
}
