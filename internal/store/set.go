package store

type Set struct {
	store *Store
}

func (set *Set) Add(key string, members ...string) int {
	entry := set.store.lookupEntry(key)
	if entry == nil {
		entry = &Entry{Type: KeyTypeSet}
		set.store.entries[key] = entry
	}
	if entry.Members == nil {
		entry.Members = make(map[string]struct{})
	}

	added := 0
	for _, member := range members {
		if _, exists := entry.Members[member]; !exists {
			entry.Members[member] = struct{}{}
			added++
		}
	}
	return added
}

func (set *Set) Remove(key string, members ...string) int {
	entry := set.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeSet {
		return 0
	}
	removed := 0
	for _, member := range members {
		if _, exists := entry.Members[member]; exists {
			delete(entry.Members, member)
			removed++
		}
	}
	return removed
}

func (set *Set) Members(key string) ([]string, bool) {
	entry := set.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeSet {
		return nil, false
	}
	members := make([]string, 0, len(entry.Members))
	for member := range entry.Members {
		members = append(members, member)
	}
	return members, true
}

func (set *Set) IsMember(key, member string) bool {
	entry := set.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeSet {
		return false
	}
	_, exists := entry.Members[member]
	return exists
}

func (set *Set) Cardinality(key string) int {
	entry := set.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeSet {
		return 0
	}
	return len(entry.Members)
}

func (set *Set) CountMissing(key string, members ...string) int {
	seen := make(map[string]struct{}, len(members))
	count := 0
	for _, member := range members {
		if _, duplicate := seen[member]; duplicate {
			continue
		}
		seen[member] = struct{}{}
		if !set.IsMember(key, member) {
			count++
		}
	}
	return count
}

func (set *Set) CountExisting(key string, members ...string) int {
	seen := make(map[string]struct{}, len(members))
	count := 0
	for _, member := range members {
		if _, duplicate := seen[member]; duplicate {
			continue
		}
		seen[member] = struct{}{}
		if set.IsMember(key, member) {
			count++
		}
	}
	return count
}

func (set *Set) Snapshot() map[string]SetEntry {
	snapshot := make(map[string]SetEntry)
	for key, entry := range set.store.entries {
		if entry.Type != KeyTypeSet {
			continue
		}
		members := make(map[string]struct{}, len(entry.Members))
		for member := range entry.Members {
			members[member] = struct{}{}
		}
		snapshot[key] = SetEntry{Members: members}
	}
	return snapshot
}
