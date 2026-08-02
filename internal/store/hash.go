package store

import "maps"

type Hash struct {
	store *Store
}

func (hash *Hash) Set(key string, fieldValues ...string) int {
	if len(fieldValues)%2 != 0 {
		return 0
	}
	entry := hash.store.lookupEntry(key)
	if entry == nil {
		entry = &Entry{Type: KeyTypeHash}
		hash.store.entries[key] = entry
	}
	if entry.Hash == nil {
		entry.Hash = make(map[string]string)
	}

	added := 0
	for index := 0; index < len(fieldValues); index += 2 {
		field, value := fieldValues[index], fieldValues[index+1]
		if _, exists := entry.Hash[field]; !exists {
			added++
		}
		entry.Hash[field] = value
	}
	return added
}

func (hash *Hash) Get(key, field string) (string, bool) {
	entry := hash.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeHash {
		return "", false
	}
	value, exists := entry.Hash[field]
	return value, exists
}

func (hash *Hash) Delete(key string, fields ...string) int {
	entry := hash.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeHash {
		return 0
	}
	deleted := 0
	for _, field := range fields {
		if _, exists := entry.Hash[field]; exists {
			delete(entry.Hash, field)
			deleted++
		}
	}
	return deleted
}

func (hash *Hash) GetAll(key string) (map[string]string, bool) {
	entry := hash.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeHash {
		return nil, false
	}
	fields := make(map[string]string, len(entry.Hash))
	maps.Copy(fields, entry.Hash)
	return fields, true
}

func (hash *Hash) Exists(key, field string) bool {
	_, exists := hash.Get(key, field)
	return exists
}

func (hash *Hash) FieldCount(key string) int {
	entry := hash.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeHash {
		return 0
	}
	return len(entry.Hash)
}

func (hash *Hash) CountMissing(key string, fieldValues ...string) int {
	seen := make(map[string]struct{}, len(fieldValues)/2)
	count := 0
	for index := 0; index < len(fieldValues); index += 2 {
		field := fieldValues[index]
		if _, duplicate := seen[field]; duplicate {
			continue
		}
		seen[field] = struct{}{}
		if !hash.Exists(key, field) {
			count++
		}
	}
	return count
}

func (hash *Hash) CountExisting(key string, fields ...string) int {
	seen := make(map[string]struct{}, len(fields))
	count := 0
	for _, field := range fields {
		if _, duplicate := seen[field]; duplicate {
			continue
		}
		seen[field] = struct{}{}
		if hash.Exists(key, field) {
			count++
		}
	}
	return count
}

func (hash *Hash) WouldChange(key string, fieldValues ...string) bool {
	updates := make(map[string]string, len(fieldValues)/2)
	for index := 0; index < len(fieldValues); index += 2 {
		updates[fieldValues[index]] = fieldValues[index+1]
	}
	for field, value := range updates {
		current, exists := hash.Get(key, field)
		if !exists || current != value {
			return true
		}
	}
	return false
}

func (hash *Hash) Snapshot() map[string]map[string]string {
	snapshot := make(map[string]map[string]string)
	for key, entry := range hash.store.entries {
		if entry.Type != KeyTypeHash {
			continue
		}
		fields := make(map[string]string, len(entry.Hash))
		maps.Copy(fields, entry.Hash)
		snapshot[key] = fields
	}
	return snapshot
}
