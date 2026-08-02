package store

type Dictionary struct {
	store *Store
}

func (dictionary *Dictionary) Set(key, value string) {
	entry := dictionary.store.lookupEntry(key)
	if entry == nil {
		entry = &Entry{Type: KeyTypeString}
		dictionary.store.entries[key] = entry
	}
	entry.Value = value
}

func (dictionary *Dictionary) Get(key string) (string, bool) {
	entry := dictionary.store.lookupEntry(key)
	if entry == nil || entry.Type != KeyTypeString {
		return "", false
	}
	return entry.Value, true
}

func (dictionary *Dictionary) Snapshot() map[string]StringEntry {
	snapshot := make(map[string]StringEntry)
	for key, entry := range dictionary.store.entries {
		if entry.Type == KeyTypeString {
			snapshot[key] = StringEntry{Value: entry.Value}
		}
	}
	return snapshot
}
