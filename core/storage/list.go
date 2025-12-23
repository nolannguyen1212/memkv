package storage

import (
	"strconv"
	"sync"
)

type List struct {
	mutex   sync.RWMutex
	entries map[string]*Deque[string]
}

func NewList() *List {
	return &List{
		entries: make(map[string]*Deque[string]),
	}
}

func (list *List) LeftPush(key string, values ...string) int {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.entries[key] == nil {
		list.entries[key] = NewDeque[string]()
	}

	for _, value := range values {
		list.entries[key].PushFront(value)
	}

	return list.entries[key].Length()
}

func (list *List) RightPush(key string, values ...string) int {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.entries[key] == nil {
		list.entries[key] = NewDeque[string]()
	}

	for _, value := range values {
		list.entries[key].PushBack(value)
	}

	return list.entries[key].Length()
}

func (list *List) LeftPop(key string, count int) []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	deque := list.entries[key]
	if deque == nil {
		return []string{}
	}

	poppedValues := make([]string, 0, count)
	for i := 0; i < count; i++ {
		value, exists := deque.PopFront()
		if !exists {
			break
		}
		poppedValues = append(poppedValues, value)
	}

	if deque.IsEmpty() {
		delete(list.entries, key)
	}

	return poppedValues
}

func (list *List) RightPop(key string, count int) []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	deque := list.entries[key]
	if deque == nil {
		return []string{}
	}

	poppedValues := make([]string, 0, count)
	for i := 0; i < count; i++ {
		value, exists := deque.PopBack()
		if !exists {
			break
		}
		poppedValues = append(poppedValues, value)
	}

	if deque.IsEmpty() {
		delete(list.entries, key)
	}

	return poppedValues
}

func (list *List) Length(key string) int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	if list.entries[key] == nil {
		return 0
	}

	return list.entries[key].Length()
}

func (list *List) Range(key string, start int, stop int) ([]string, bool) {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	deque := list.entries[key]
	if deque == nil || deque.Length() == 0 {
		return []string{}, false
	}

	length := deque.Length()
	normalizedStart, normalizedStop := normalizeRange(start, stop, length)

	if normalizedStart > normalizedStop {
		return []string{}, true
	}

	values := make([]string, 0, normalizedStop-normalizedStart+1)
	for i := normalizedStart; i <= normalizedStop; i++ {
		values = append(values, deque.ElementAt(i))
	}

	return values, true
}

func (list *List) Sort(key string, ascending bool, alphabetical bool) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.entries[key] == nil {
		return
	}

	list.entries[key].Sort(func(first, second string) bool {
		isLess := compareStrings(first, second, alphabetical)
		if ascending {
			return isLess
		}
		return !isLess
	})
}

func (list *List) Snapshot() map[string][]string {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	snapshot := make(map[string][]string, len(list.entries))

	for key, deque := range list.entries {
		if deque == nil || deque.Length() == 0 {
			continue
		}

		values := make([]string, deque.Length())
		for i := 0; i < deque.Length(); i++ {
			values[i] = deque.ElementAt(i)
		}
		snapshot[key] = values
	}

	return snapshot
}

func normalizeRange(start, stop, length int) (int, int) {
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
	return start, stop
}

func compareStrings(first, second string, alphabetical bool) bool {
	if alphabetical {
		return first < second
	}

	firstNumber, firstErr := strconv.ParseFloat(first, 64)
	secondNumber, secondErr := strconv.ParseFloat(second, 64)

	if firstErr != nil || secondErr != nil {
		return first < second
	}

	return firstNumber < secondNumber
}
