package store

import "sort"

type ScoreMember struct {
	Score  float64
	Member string
}

type sortedSetEntry struct {
	scores map[string]float64
	items  []ScoreMember
}

type SortedSet struct {
	store *Store
}

func (sortedSet *SortedSet) Add(key string, items ...ScoreMember) int {
	entry := sortedSet.store.lookupEntry(key)

	if entry == nil {
		entry = sortedSet.store.createEntry(key, KeyTypeSortedSet)
	}

	if entry.SortedSet == nil {
		entry.SortedSet = &sortedSetEntry{scores: make(map[string]float64)}
	}

	added := 0
	changed := false

	for _, item := range items {
		oldScore, exists := entry.SortedSet.scores[item.Member]

		if !exists {
			added++
			changed = true
		} else if oldScore != item.Score {
			changed = true
		}

		entry.SortedSet.scores[item.Member] = item.Score
	}

	if changed {
		entry.SortedSet.rebuild()
		sortedSet.store.Keyspace.BumpVersion(key)
	}

	return added
}

func (sortedSet *SortedSet) Remove(key string, members ...string) int {
	entry := sortedSet.entry(key)

	if entry == nil {
		return 0
	}

	removed := 0

	for _, member := range members {
		if _, exists := entry.scores[member]; exists {
			delete(entry.scores, member)
			removed++
		}
	}

	if removed > 0 {
		entry.rebuild()
		sortedSet.store.Keyspace.BumpVersion(key)
	}

	if len(entry.scores) == 0 {
		sortedSet.store.removeEmptyKey(key, KeyTypeSortedSet)
	}

	return removed
}

func (sortedSet *SortedSet) Score(key, member string) (float64, bool) {
	entry := sortedSet.entry(key)

	if entry == nil {
		return 0, false
	}

	score, exists := entry.scores[member]
	return score, exists
}

func (sortedSet *SortedSet) Rank(key, member string) (int, bool) {
	entry := sortedSet.entry(key)

	if entry == nil {
		return 0, false
	}

	score, exists := entry.scores[member]

	if !exists {
		return 0, false
	}

	rank := sort.Search(len(entry.items), func(index int) bool {
		item := entry.items[index]
		return item.Score > score || item.Score == score && item.Member >= member
	})
	return rank, rank < len(entry.items) && entry.items[rank].Member == member
}

func (sortedSet *SortedSet) Cardinality(key string) int {
	entry := sortedSet.entry(key)

	if entry == nil {
		return 0
	}

	return len(entry.items)
}

func (sortedSet *SortedSet) CountMissing(key string, items ...ScoreMember) int {
	seen := make(map[string]struct{}, len(items))
	count := 0

	for _, item := range items {
		if _, duplicate := seen[item.Member]; duplicate {
			continue
		}

		seen[item.Member] = struct{}{}

		if _, exists := sortedSet.Score(key, item.Member); !exists {
			count++
		}
	}

	return count
}

func (sortedSet *SortedSet) CountExisting(key string, members ...string) int {
	seen := make(map[string]struct{}, len(members))
	count := 0

	for _, member := range members {
		if _, duplicate := seen[member]; duplicate {
			continue
		}

		seen[member] = struct{}{}

		if _, exists := sortedSet.Score(key, member); exists {
			count++
		}
	}

	return count
}

func (sortedSet *SortedSet) WouldChange(key string, items ...ScoreMember) bool {
	updates := make(map[string]float64, len(items))

	for _, item := range items {
		updates[item.Member] = item.Score
	}

	for member, score := range updates {
		current, exists := sortedSet.Score(key, member)

		if !exists || current != score {
			return true
		}
	}

	return false
}

func (sortedSet *SortedSet) Range(key string, start, stop int) []string {
	entry := sortedSet.entry(key)

	if entry == nil {
		return []string{}
	}

	length := len(entry.items)

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
		return []string{}
	}

	if start >= length {
		return []string{}
	}

	if stop < 0 {
		return []string{}
	}

	result := make([]string, stop-start+1)

	for index, item := range entry.items[start : stop+1] {
		result[index] = item.Member
	}

	return result
}

func (sortedSet *SortedSet) Snapshot() map[string]map[string]float64 {
	snapshot := make(map[string]map[string]float64)

	for key, entry := range sortedSet.store.entries {
		if entry.Type != KeyTypeSortedSet || entry.SortedSet == nil {
			continue
		}

		members := make(map[string]float64, len(entry.SortedSet.scores))

		for member, score := range entry.SortedSet.scores {
			members[member] = score
		}

		snapshot[key] = members
	}

	return snapshot
}

func (sortedSet *SortedSet) entry(key string) *sortedSetEntry {
	entry := sortedSet.store.lookupEntry(key)

	if entry == nil || entry.Type != KeyTypeSortedSet {
		return nil
	}

	return entry.SortedSet
}

func (entry *sortedSetEntry) rebuild() {
	entry.items = make([]ScoreMember, 0, len(entry.scores))

	for member, score := range entry.scores {
		entry.items = append(entry.items, ScoreMember{Score: score, Member: member})
	}

	sort.Slice(entry.items, func(i, j int) bool {
		if entry.items[i].Score == entry.items[j].Score {
			return entry.items[i].Member < entry.items[j].Member
		}

		return entry.items[i].Score < entry.items[j].Score
	})
}
