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

func (evictionManager *EvictionManager) IsEnabled() bool {
	return evictionManager.keyLimit != nil && *evictionManager.keyLimit > 0 && evictionManager.strategy != EvictNone
}

func (evictionManager *EvictionManager) RecordInsert(key string) {
	if evictionManager.keyLimit == nil {
		return
	}

	evictionManager.mutex.Lock()
	defer evictionManager.mutex.Unlock()

	if _, exists := evictionManager.metadata[key]; exists {
		return
	}

	evictionManager.insertOrder++
	evictionManager.metadata[key] = &keyMeta{
		key:         key,
		accessTime:  time.Now(),
		accessCount: 1,
		insertOrder: evictionManager.insertOrder,
	}

	if evictionManager.strategy == EvictLRU {
		element := evictionManager.lruList.PushFront(key)
		evictionManager.lruElements[key] = element
	}
}

func (evictionManager *EvictionManager) RecordAccess(key string) {
	if evictionManager.keyLimit == nil {
		return
	}

	evictionManager.mutex.Lock()
	defer evictionManager.mutex.Unlock()

	meta, exists := evictionManager.metadata[key]
	if !exists {
		return
	}

	meta.accessTime = time.Now()
	meta.accessCount++

	if evictionManager.strategy == EvictLRU {
		if element, ok := evictionManager.lruElements[key]; ok {
			evictionManager.lruList.MoveToFront(element)
		}
	}
}

func (evictionManager *EvictionManager) RecordDelete(key string) {
	if evictionManager.keyLimit == nil {
		return
	}

	evictionManager.mutex.Lock()
	defer evictionManager.mutex.Unlock()

	delete(evictionManager.metadata, key)
	if element, exists := evictionManager.lruElements[key]; exists {
		evictionManager.lruList.Remove(element)
		delete(evictionManager.lruElements, key)
	}
}

func (evictionManager *EvictionManager) ShouldEvict() bool {
	if !evictionManager.IsEnabled() {
		return false
	}

	evictionManager.mutex.RLock()
	defer evictionManager.mutex.RUnlock()

	return len(evictionManager.metadata) > *evictionManager.keyLimit
}

func (evictionManager *EvictionManager) EvictOne() string {
	if !evictionManager.IsEnabled() {
		return ""
	}

	evictionManager.mutex.Lock()
	defer evictionManager.mutex.Unlock()

	if len(evictionManager.metadata) <= *evictionManager.keyLimit {
		return ""
	}

	var keyToEvict string

	switch evictionManager.strategy {
	case EvictLRU:
		keyToEvict = evictionManager.findLRU()
	case EvictLFU:
		keyToEvict = evictionManager.findLFU()
	case EvictFirst:
		keyToEvict = evictionManager.findFirst()
	default:
		return ""
	}

	if keyToEvict == "" {
		return ""
	}

	delete(evictionManager.metadata, keyToEvict)
	if element, exists := evictionManager.lruElements[keyToEvict]; exists {
		evictionManager.lruList.Remove(element)
		delete(evictionManager.lruElements, keyToEvict)
	}

	if evictionManager.onEvict != nil {
		evictionManager.onEvict(keyToEvict)
	}

	return keyToEvict
}

func (evictionManager *EvictionManager) findLRU() string {
	if evictionManager.lruList.Len() == 0 {
		return ""
	}
	element := evictionManager.lruList.Back()
	if element == nil {
		return ""
	}
	return element.Value.(string)
}

func (evictionManager *EvictionManager) findLFU() string {
	var minKey string
	var minCount int64 = -1

	for key, meta := range evictionManager.metadata {
		if minCount == -1 || meta.accessCount < minCount {
			minCount = meta.accessCount
			minKey = key
		}
	}
	return minKey
}

func (evictionManager *EvictionManager) findFirst() string {
	var oldestKey string
	var oldestOrder int64 = -1

	for key, meta := range evictionManager.metadata {
		if oldestOrder == -1 || meta.insertOrder < oldestOrder {
			oldestOrder = meta.insertOrder
			oldestKey = key
		}
	}
	return oldestKey
}

func (evictionManager *EvictionManager) KeyCount() int {
	evictionManager.mutex.RLock()
	defer evictionManager.mutex.RUnlock()
	return len(evictionManager.metadata)
}
