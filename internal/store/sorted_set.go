package store

import (
	"math/rand"
	"sync"
)

const (
	skipListMaxLevel    = 32
	skipListProbability = 0.25
)

type ScoreMember struct {
	Score  float64
	Member string
}

type skipNode struct {
	member  string
	score   float64
	forward []*skipNode
	span    []int
}

type skipList struct {
	header *skipNode
	tail   *skipNode
	length int
	level  int
}

type sortedSetEntry struct {
	list        *skipList
	memberScore map[string]float64
}

type SortedSet struct {
	mutex   sync.RWMutex
	entries map[string]*sortedSetEntry
}

func NewSortedSet() *SortedSet {
	return &SortedSet{entries: make(map[string]*sortedSetEntry)}
}

func newSkipNode(level int, score float64, member string) *skipNode {
	return &skipNode{
		member:  member,
		score:   score,
		forward: make([]*skipNode, level),
		span:    make([]int, level),
	}
}

func newSkipList() *skipList {
	return &skipList{
		header: newSkipNode(skipListMaxLevel, 0, ""),
		level:  1,
	}
}

func randomLevel() int {
	level := 1
	for rand.Float64() < skipListProbability && level < skipListMaxLevel {
		level++
	}
	return level
}

func (sl *skipList) insert(score float64, member string) {
	update := make([]*skipNode, skipListMaxLevel)
	rank := make([]int, skipListMaxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for current.forward[i] != nil &&
			(current.forward[i].score < score ||
				(current.forward[i].score == score && current.forward[i].member < member)) {
			rank[i] += current.span[i]
			current = current.forward[i]
		}
		update[i] = current
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].span[i] = sl.length
		}
		sl.level = level
	}

	node := newSkipNode(level, score, member)
	for i := 0; i < level; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
		node.span[i] = update[i].span[i] - (rank[0] - rank[i])
		update[i].span[i] = (rank[0] - rank[i]) + 1
	}
	for i := level; i < sl.level; i++ {
		update[i].span[i]++
	}

	if node.forward[0] == nil {
		sl.tail = node
	}
	sl.length++
}

func (sl *skipList) delete(score float64, member string) bool {
	update := make([]*skipNode, skipListMaxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			(current.forward[i].score < score ||
				(current.forward[i].score == score && current.forward[i].member < member)) {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current == nil || current.score != score || current.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] == current {
			update[i].span[i] += current.span[i] - 1
			update[i].forward[i] = current.forward[i]
		} else {
			update[i].span[i]--
		}
	}

	for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
		sl.level--
	}
	sl.length--
	return true
}

func (sl *skipList) getRank(score float64, member string) int {
	rank := 0
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			(current.forward[i].score < score ||
				(current.forward[i].score == score && current.forward[i].member <= member)) {
			rank += current.span[i]
			current = current.forward[i]
		}
		if current.member == member && current.score == score {
			return rank
		}
	}
	return 0
}

func (sortedSet *SortedSet) Add(key string, items ...ScoreMember) int {
	sortedSet.mutex.Lock()
	defer sortedSet.mutex.Unlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		entry = &sortedSetEntry{
			list:        newSkipList(),
			memberScore: make(map[string]float64),
		}
		sortedSet.entries[key] = entry
	}

	added := 0
	for _, item := range items {
		if oldScore, exists := entry.memberScore[item.Member]; exists {
			if oldScore != item.Score {
				entry.list.delete(oldScore, item.Member)
				entry.list.insert(item.Score, item.Member)
				entry.memberScore[item.Member] = item.Score
			}
		} else {
			entry.list.insert(item.Score, item.Member)
			entry.memberScore[item.Member] = item.Score
			added++
		}
	}
	return added
}

func (sortedSet *SortedSet) Remove(key string, members ...string) int {
	sortedSet.mutex.Lock()
	defer sortedSet.mutex.Unlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		return 0
	}

	removed := 0
	for _, member := range members {
		if score, exists := entry.memberScore[member]; exists {
			entry.list.delete(score, member)
			delete(entry.memberScore, member)
			removed++
		}
	}
	if len(entry.memberScore) == 0 {
		delete(sortedSet.entries, key)
	}
	return removed
}

func (sortedSet *SortedSet) DeleteKey(key string) bool {
	sortedSet.mutex.Lock()
	defer sortedSet.mutex.Unlock()
	if _, exists := sortedSet.entries[key]; !exists {
		return false
	}
	delete(sortedSet.entries, key)
	return true
}

func (sortedSet *SortedSet) Score(key, member string) (float64, bool) {
	sortedSet.mutex.RLock()
	defer sortedSet.mutex.RUnlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		return 0, false
	}
	score, exists := entry.memberScore[member]
	return score, exists
}

func (sortedSet *SortedSet) Rank(key, member string) (int, bool) {
	sortedSet.mutex.RLock()
	defer sortedSet.mutex.RUnlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		return 0, false
	}
	score, exists := entry.memberScore[member]
	if !exists {
		return 0, false
	}
	rank := entry.list.getRank(score, member)
	if rank == 0 {
		return 0, false
	}
	return rank - 1, true
}

func (sortedSet *SortedSet) Cardinality(key string) int {
	sortedSet.mutex.RLock()
	defer sortedSet.mutex.RUnlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		return 0
	}
	return entry.list.length
}

func (sortedSet *SortedSet) Range(key string, start, stop int) []string {
	sortedSet.mutex.RLock()
	defer sortedSet.mutex.RUnlock()

	entry, exists := sortedSet.entries[key]
	if !exists {
		return []string{}
	}

	length := entry.list.length
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
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

	result := make([]string, 0, stop-start+1)
	current := entry.list.header.forward[0]
	for i := 0; i < start && current != nil; i++ {
		current = current.forward[0]
	}
	for i := start; i <= stop && current != nil; i++ {
		result = append(result, current.member)
		current = current.forward[0]
	}
	return result
}

func (sortedSet *SortedSet) Snapshot() map[string]map[string]float64 {
	sortedSet.mutex.RLock()
	defer sortedSet.mutex.RUnlock()

	snapshot := make(map[string]map[string]float64, len(sortedSet.entries))
	for key, entry := range sortedSet.entries {
		membersCopy := make(map[string]float64, len(entry.memberScore))
		for member, score := range entry.memberScore {
			membersCopy[member] = score
		}
		snapshot[key] = membersCopy
	}
	return snapshot
}
