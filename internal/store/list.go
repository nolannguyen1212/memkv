package store

type List struct {
	store *Store
}

func (list *List) LeftPush(key string, values ...string) int {
	entry := list.entry(key)
	for _, value := range values {
		entry.List.PushFront(value)
	}
	return entry.List.Length()
}

func (list *List) RightPush(key string, values ...string) int {
	entry := list.entry(key)
	for _, value := range values {
		entry.List.PushBack(value)
	}
	return entry.List.Length()
}

func (list *List) entry(key string) *Entry {
	entry := list.store.lookupEntry(key)
	if entry == nil {
		entry = &Entry{Type: KeyTypeList}
		list.store.entries[key] = entry
	}
	if entry.List == nil {
		entry.List = NewDeque[string]()
	}
	return entry
}

func (list *List) LeftPop(key string, count int) []string {
	return list.pop(key, count, func(values *Deque[string]) (string, bool) {
		return values.PopFront()
	})
}

func (list *List) RightPop(key string, count int) []string {
	return list.pop(key, count, func(values *Deque[string]) (string, bool) {
		return values.PopBack()
	})
}

func (list *List) pop(key string, count int, pop func(*Deque[string]) (string, bool)) []string {
	entry := list.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeList || entry.List == nil {
		return []string{}
	}
	values := make([]string, 0, count)
	for range count {
		value, exists := pop(entry.List)
		if !exists {
			break
		}
		values = append(values, value)
	}
	return values
}

func (list *List) Length(key string) int {
	entry := list.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeList || entry.List == nil {
		return 0
	}
	return entry.List.Length()
}

func (list *List) PeekLeft(key string, count int) []string {
	entry := list.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeList || entry.List == nil || count <= 0 {
		return []string{}
	}
	if count > entry.List.Length() {
		count = entry.List.Length()
	}
	values := make([]string, count)
	for index := range count {
		values[index] = entry.List.At(index)
	}
	return values
}

func (list *List) PeekRight(key string, count int) []string {
	entry := list.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeList || entry.List == nil || count <= 0 {
		return []string{}
	}
	if count > entry.List.Length() {
		count = entry.List.Length()
	}
	values := make([]string, count)
	for index := range count {
		values[index] = entry.List.At(entry.List.Length() - index - 1)
	}
	return values
}

func (list *List) Range(key string, start, stop int) ([]string, bool) {
	entry := list.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeList || entry.List == nil || entry.List.IsEmpty() {
		return []string{}, false
	}

	length := entry.List.Length()
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

	values := make([]string, 0, stop-start+1)
	for index := start; index <= stop; index++ {
		values = append(values, entry.List.At(index))
	}
	return values, true
}

func (list *List) Snapshot() map[string][]string {
	snapshot := make(map[string][]string)
	for key, entry := range list.store.entries {
		if entry.Type != KeyTypeList || entry.List == nil || entry.List.IsEmpty() {
			continue
		}
		values := make([]string, entry.List.Length())
		for index := range values {
			values[index] = entry.List.At(index)
		}
		snapshot[key] = values
	}
	return snapshot
}
