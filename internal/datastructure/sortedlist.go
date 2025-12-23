package datastructure

import (
	"math/rand"
	"sync"
)

const (
	skipListMaxLevel    = 32
	skipListProbability = 0.25
)

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

type sortedEntry struct {
	list        *skipList
	memberScore map[string]float64
}

type SortedList struct {
	mutex   sync.RWMutex
	entries map[string]*sortedEntry
}

func NewSortedList() *SortedList {
	return &SortedList{entries: make(map[string]*sortedEntry)}
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
	curr := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for curr.forward[i] != nil &&
			(curr.forward[i].score < score ||
				(curr.forward[i].score == score && curr.forward[i].member < member)) {
			rank[i] += curr.span[i]
			curr = curr.forward[i]
		}
		update[i] = curr
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
	curr := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil &&
			(curr.forward[i].score < score ||
				(curr.forward[i].score == score && curr.forward[i].member < member)) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]
	if curr == nil || curr.score != score || curr.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] == curr {
			update[i].span[i] += curr.span[i] - 1
			update[i].forward[i] = curr.forward[i]
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
	curr := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil &&
			(curr.forward[i].score < score ||
				(curr.forward[i].score == score && curr.forward[i].member <= member)) {
			rank += curr.span[i]
			curr = curr.forward[i]
		}
		if curr.member == member && curr.score == score {
			return rank
		}
	}
	return 0
}

func (s *SortedList) Add(key string, scoreMembers ...interface{}) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(scoreMembers)%2 != 0 {
		return 0
	}

	entry, exists := s.entries[key]
	if !exists {
		entry = &sortedEntry{
			list:        newSkipList(),
			memberScore: make(map[string]float64),
		}
		s.entries[key] = entry
	}

	added := 0
	for i := 0; i < len(scoreMembers); i += 2 {
		score, ok1 := scoreMembers[i].(float64)
		member, ok2 := scoreMembers[i+1].(string)
		if !ok1 || !ok2 {
			continue
		}

		if oldScore, exists := entry.memberScore[member]; exists {
			if oldScore != score {
				entry.list.delete(oldScore, member)
				entry.list.insert(score, member)
				entry.memberScore[member] = score
			}
		} else {
			entry.list.insert(score, member)
			entry.memberScore[member] = score
			added++
		}
	}
	return added
}

func (s *SortedList) Remove(key string, members ...string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entry, exists := s.entries[key]
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
		delete(s.entries, key)
	}
	return removed
}

func (s *SortedList) Score(key, member string) (float64, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, exists := s.entries[key]
	if !exists {
		return 0, false
	}
	score, exists := entry.memberScore[member]
	return score, exists
}

func (s *SortedList) Rank(key, member string) (int, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, exists := s.entries[key]
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

func (s *SortedList) Cardinality(key string) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, exists := s.entries[key]
	if !exists {
		return 0
	}
	return entry.list.length
}

func (s *SortedList) Range(key string, start, stop int) []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, exists := s.entries[key]
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
	curr := entry.list.header.forward[0]
	for i := 0; i < start && curr != nil; i++ {
		curr = curr.forward[0]
	}
	for i := start; i <= stop && curr != nil; i++ {
		result = append(result, curr.member)
		curr = curr.forward[0]
	}
	return result
}

func (s *SortedList) Snapshot() map[string]map[string]float64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	snapshot := make(map[string]map[string]float64, len(s.entries))
	for key, entry := range s.entries {
		membersCopy := make(map[string]float64, len(entry.memberScore))
		for member, score := range entry.memberScore {
			membersCopy[member] = score
		}
		snapshot[key] = membersCopy
	}
	return snapshot
}
