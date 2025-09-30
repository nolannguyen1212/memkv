package datastructure

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Dict struct {
	mu    sync.RWMutex
	items map[string]Item
}

func CreateDict() *Dict {
	d := &Dict{
		items: make(map[string]Item),
	}
	go d.expireLoop()
	return d
}

func (d *Dict) getItem(key string) (Item, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	item, exist := d.items[key]
	return item, exist
}

func (d *Dict) isExpired(item Item) bool {
	return !item.ExpiredAt.IsZero() && time.Now().After(item.ExpiredAt)
}

func (d *Dict) passiveExpire(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.items, key)
}

func (d *Dict) Set(key, value string, ttl time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	item := Item{Value: value}
	if ttl > 0 {
		item.ExpiredAt = time.Now().Add(ttl)
	}
	d.items[key] = item
}

func (d *Dict) Get(key string) (string, bool) {
	item, exist := d.getItem(key)
	if !exist {
		return "", false
	}
	if d.isExpired(item) {
		d.passiveExpire(key)
		return "", false
	}
	return item.Value, true
}

func (d *Dict) Delete(keys ...string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	count := 0
	for _, key := range keys {
		if _, exist := d.items[key]; exist {
			delete(d.items, key)
			count++
		}
	}
	return count
}

func (d *Dict) Expire(key string, ttl time.Duration) bool {
	item, exist := d.getItem(key)
	if !exist {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if ttl <= 0 {
		delete(d.items, key)
		return true
	}

	item.ExpiredAt = time.Now().Add(ttl)
	d.items[key] = item

	return true
}

func (d *Dict) ExpireAt(key string, at time.Time) bool {
	item, exist := d.getItem(key)
	if !exist {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if at.Before(time.Now()) {
		delete(d.items, key)
		return true
	}

	item.ExpiredAt = at
	d.items[key] = item

	return true
}

func (d *Dict) TTL(key string) int64 {
	item, exist := d.getItem(key)
	if !exist {
		return -2
	}

	if item.ExpiredAt.IsZero() {
		return -1
	}

	seconds := time.Until(item.ExpiredAt).Seconds()
	if seconds < 0 {
		d.passiveExpire(key)
		return -2
	}

	return int64(seconds)
}

func (d *Dict) expireLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		d.activeExpire()
	}
}

func (d *Dict) activeExpire() {
	if len(d.items) == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for i := 0; i < maxSampleRounds; i++ {
		checked := 0
		expired := 0

		keys := make([]string, 0, len(d.items))
		for key := range d.items {
			keys = append(keys, key)
		}

		limit := maxSampleSize
		if len(keys) < limit {
			limit = len(keys)
		}

		for j := 0; j < limit; j++ {
			idx := rand.IntN(len(keys))
			key := keys[idx]
			if item, exist := d.items[key]; exist {
				checked++
				if d.isExpired(item) {
					delete(d.items, key)
					expired++
				}
			}
		}

		if checked == 0 || expired*4 < checked {
			return
		}
	}
}

func (d *Dict) Dump() map[string]Item {
	d.mu.RLock()
	defer d.mu.RUnlock()

	snapshot := make(map[string]Item, len(d.items))

	for key, item := range d.items {
		if !d.isExpired(item) {
			snapshot[key] = item
		}
	}

	return snapshot
}
