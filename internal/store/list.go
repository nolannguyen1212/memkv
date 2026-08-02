package store

type List struct {
	store *Store
}

func (list *List) LeftPush(key string, values ...string) int {
	entry := list.entry(key)

	for _, value := range values {
		entry.List.PushFront(value)
	}

	if len(values) > 0 {
		list.store.Keyspace.BumpVersion(key)
	}

	return entry.List.Length()
}

func (list *List) RightPush(key string, values ...string) int {
	entry := list.entry(key)

	for _, value := range values {
		entry.List.PushBack(value)
	}

	if len(values) > 0 {
		list.store.Keyspace.BumpVersion(key)
	}

	return entry.List.Length()
}

func (list *List) entry(key string) *Entry {
	entry := list.store.lookupEntry(key)

	if entry == nil {
		entry = list.store.createEntry(key, KeyTypeList)
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

	if !isListEntry(entry) {
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

	if len(values) > 0 {
		list.store.Keyspace.BumpVersion(key)
	}

	if entry.List.IsEmpty() {
		list.store.removeEmptyKey(key, KeyTypeList)
	}

	return values
}

func (list *List) Length(key string) int {
	entry := list.store.lookupEntry(key)

	if !isListEntry(entry) {
		return 0
	}

	return entry.List.Length()
}

func (list *List) PeekLeft(key string, count int) []string {
	entry := list.store.lookupEntry(key)

	if !isListEntry(entry) {
		return []string{}
	}

	if count <= 0 {
		return []string{}
	}

	if count > entry.List.Length() {
		count = entry.List.Length()
	}

	values := make([]string, count)

	for index := range count {
		values[index] = entry.List.at(index)
	}

	return values
}

func (list *List) PeekRight(key string, count int) []string {
	entry := list.store.lookupEntry(key)

	if !isListEntry(entry) {
		return []string{}
	}

	if count <= 0 {
		return []string{}
	}

	if count > entry.List.Length() {
		count = entry.List.Length()
	}

	values := make([]string, count)

	for index := range count {
		values[index] = entry.List.at(entry.List.Length() - index - 1)
	}

	return values
}

func (list *List) Range(key string, start, stop int) ([]string, bool) {
	entry := list.store.lookupEntry(key)

	if !isListEntry(entry) {
		return []string{}, false
	}

	if entry.List.IsEmpty() {
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
		values = append(values, entry.List.at(index))
	}

	return values, true
}

func (list *List) Snapshot() map[string][]string {
	snapshot := make(map[string][]string)

	for key, entry := range list.store.entries {
		if !isListEntry(entry) {
			continue
		}

		if entry.List.IsEmpty() {
			continue
		}

		values := make([]string, entry.List.Length())

		for index := range values {
			values[index] = entry.List.at(index)
		}

		snapshot[key] = values
	}

	return snapshot
}

func isListEntry(entry *Entry) bool {
	if entry == nil {
		return false
	}

	if entry.Type != KeyTypeList {
		return false
	}

	return entry.List != nil
}
