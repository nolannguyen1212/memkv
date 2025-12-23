package datastructure

import (
	"container/list"
	"sync"
	"time"
)

type EvictStrategy string

const (
	EvictNone  EvictStrategy = ""
	EvictLRU   EvictStrategy = "lru"
	EvictLFU   EvictStrategy = "lfu"
	EvictFirst EvictStrategy = "evict_first"
)

type keyMeta struct {
	key         string
	accessTime  time.Time
	accessCount int64
	insertOrder int64
}

type EvictionManager struct {
	mutex       sync.RWMutex
	strategy    EvictStrategy
	keyLimit    *int
	metadata    map[string]*keyMeta
	lruList     *list.List
	lruElements map[string]*list.Element
	insertOrder int64
	onEvict     func(key string)
}

type EvictionConfig struct {
	Strategy EvictStrategy
	KeyLimit *int
	OnEvict  func(key string)
}

func NewEvictionManager(config EvictionConfig) *EvictionManager {
	return &EvictionManager{
		strategy:    config.Strategy,
		keyLimit:    config.KeyLimit,
		metadata:    make(map[string]*keyMeta),
		lruList:     list.New(),
		lruElements: make(map[string]*list.Element),
		onEvict:     config.OnEvict,
	}
}

func (em *EvictionManager) IsEnabled() bool {
	return em.keyLimit != nil && *em.keyLimit > 0 && em.strategy != EvictNone
}

func (em *EvictionManager) RecordInsert(key string) {
	if em.keyLimit == nil {
		return
	}

	em.mutex.Lock()
	defer em.mutex.Unlock()

	if _, exists := em.metadata[key]; exists {
		return
	}

	em.insertOrder++
	em.metadata[key] = &keyMeta{
		key:         key,
		accessTime:  time.Now(),
		accessCount: 1,
		insertOrder: em.insertOrder,
	}

	if em.strategy == EvictLRU {
		elem := em.lruList.PushFront(key)
		em.lruElements[key] = elem
	}
}

func (em *EvictionManager) RecordAccess(key string) {
	if em.keyLimit == nil {
		return
	}

	em.mutex.Lock()
	defer em.mutex.Unlock()

	meta, exists := em.metadata[key]
	if !exists {
		return
	}

	meta.accessTime = time.Now()
	meta.accessCount++

	if em.strategy == EvictLRU {
		if elem, ok := em.lruElements[key]; ok {
			em.lruList.MoveToFront(elem)
		}
	}
}

func (em *EvictionManager) RecordDelete(key string) {
	if em.keyLimit == nil {
		return
	}

	em.mutex.Lock()
	defer em.mutex.Unlock()

	delete(em.metadata, key)
	if elem, exists := em.lruElements[key]; exists {
		em.lruList.Remove(elem)
		delete(em.lruElements, key)
	}
}

func (em *EvictionManager) ShouldEvict() bool {
	if !em.IsEnabled() {
		return false
	}

	em.mutex.RLock()
	defer em.mutex.RUnlock()

	return len(em.metadata) > *em.keyLimit
}

func (em *EvictionManager) EvictOne() string {
	if !em.IsEnabled() {
		return ""
	}

	em.mutex.Lock()
	defer em.mutex.Unlock()

	if len(em.metadata) <= *em.keyLimit {
		return ""
	}

	var keyToEvict string

	switch em.strategy {
	case EvictLRU:
		keyToEvict = em.findLRU()
	case EvictLFU:
		keyToEvict = em.findLFU()
	case EvictFirst:
		keyToEvict = em.findFirst()
	default:
		return ""
	}

	if keyToEvict == "" {
		return ""
	}

	delete(em.metadata, keyToEvict)
	if elem, exists := em.lruElements[keyToEvict]; exists {
		em.lruList.Remove(elem)
		delete(em.lruElements, keyToEvict)
	}

	if em.onEvict != nil {
		em.onEvict(keyToEvict)
	}

	return keyToEvict
}

func (em *EvictionManager) findLRU() string {
	if em.lruList.Len() == 0 {
		return ""
	}
	elem := em.lruList.Back()
	if elem == nil {
		return ""
	}
	return elem.Value.(string)
}

func (em *EvictionManager) findLFU() string {
	var minKey string
	var minCount int64 = -1

	for key, meta := range em.metadata {
		if minCount == -1 || meta.accessCount < minCount {
			minCount = meta.accessCount
			minKey = key
		}
	}
	return minKey
}

func (em *EvictionManager) findFirst() string {
	var oldestKey string
	var oldestOrder int64 = -1

	for key, meta := range em.metadata {
		if oldestOrder == -1 || meta.insertOrder < oldestOrder {
			oldestOrder = meta.insertOrder
			oldestKey = key
		}
	}
	return oldestKey
}

func (em *EvictionManager) KeyCount() int {
	em.mutex.RLock()
	defer em.mutex.RUnlock()
	return len(em.metadata)
}
