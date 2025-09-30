package datastructure

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Set struct {
	mu    sync.RWMutex
	items map[string]Item
}

func CreateSet() *Set {
	s := &Set{items: make(map[string]Item)}
	go s.expireLoop()
	return s
}

func (s *Set) getItem(key string) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[key]
	return item, ok
}

func (s *Set) isExpired(it Item) bool {
	return !it.ExpiredAt.IsZero() && time.Now().After(it.ExpiredAt)
}

func (s *Set) passiveExpire(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

func (s *Set) Sadd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		item = Item{Members: make(map[string]struct{})}
	}
	added := 0
	for _, m := range members {
		if _, exist := item.Members[m]; !exist {
			item.Members[m] = struct{}{}
			added++
		}
	}
	s.items[key] = item
	return added
}

func (s *Set) Srem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok || len(item.Members) == 0 {
		return 0
	}
	removed := 0
	for _, m := range members {
		if _, exist := item.Members[m]; exist {
			delete(item.Members, m)
			removed++
		}
	}
	if len(item.Members) == 0 {
		delete(s.items, key)
	} else {
		s.items[key] = item
	}
	return removed
}

func (s *Set) Smembers(key string) ([]string, bool) {
	item, ok := s.getItem(key)
	if !ok {
		return nil, false
	}
	if s.isExpired(item) {
		s.passiveExpire(key)
		return nil, false
	}
	res := make([]string, 0, len(item.Members))
	for m := range item.Members {
		res = append(res, m)
	}
	return res, true
}

func (s *Set) Sismember(key, member string) bool {
	item, ok := s.getItem(key)
	if !ok {
		return false
	}
	if s.isExpired(item) {
		s.passiveExpire(key)
		return false
	}
	_, exist := item.Members[member]
	return exist
}

func (s *Set) Scard(key string) int {
	item, ok := s.getItem(key)
	if !ok {
		return 0
	}
	if s.isExpired(item) {
		s.passiveExpire(key)
		return 0
	}
	return len(item.Members)
}

func (s *Set) Expire(key string, ttl time.Duration) bool {
	item, ok := s.getItem(key)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ttl <= 0 {
		delete(s.items, key)
		return true
	}
	item.ExpiredAt = time.Now().Add(ttl)
	s.items[key] = item
	return true
}

func (s *Set) ExpireAt(key string, at time.Time) bool {
	item, ok := s.getItem(key)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if at.Before(time.Now()) {
		delete(s.items, key)
		return true
	}
	item.ExpiredAt = at
	s.items[key] = item
	return true
}

func (s *Set) TTL(key string) int64 {
	item, ok := s.getItem(key)
	if !ok {
		return -2
	}
	if item.ExpiredAt.IsZero() {
		return -1
	}
	secs := time.Until(item.ExpiredAt).Seconds()
	if secs < 0 {
		s.passiveExpire(key)
		return -2
	}
	return int64(secs)
}

func (s *Set) Dump() map[string]Item {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string]Item, len(s.items))
	for k, it := range s.items {
		if s.isExpired(it) {
			continue
		}
		members := make(map[string]struct{}, len(it.Members))
		for m := range it.Members {
			members[m] = struct{}{}
		}
		snapshot[k] = Item{Members: members, ExpiredAt: it.ExpiredAt}
	}
	return snapshot
}

func (s *Set) expireLoop() {
	ticker := time.NewTicker(GetExpirationCheckInterval())
	defer ticker.Stop()
	for range ticker.C {
		s.activeExpire()
	}
}

func (s *Set) activeExpire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		return
	}
	for range GetMaxSampleRounds() {
		checked, expired := 0, 0
		keys := make([]string, 0, len(s.items))
		for k := range s.items {
			keys = append(keys, k)
		}
		if len(keys) == 0 {
			return
		}
		limit := GetMaxSampleSize()
		if len(keys) < limit {
			limit = len(keys)
		}
		for i := 0; i < limit; i++ {
			idx := rand.IntN(len(keys))
			k := keys[idx]
			if item, ok := s.items[k]; ok {
				checked++
				if s.isExpired(item) {
					delete(s.items, k)
					expired++
				}
			}
		}
		if checked == 0 || expired*4 < checked {
			return
		}
	}
}
