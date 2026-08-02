package store

import (
	"container/list"
)

type EvictStrategy string

const (
	EvictNone EvictStrategy = ""
	EvictLRU  EvictStrategy = "lru"
)

type EvictionConfig struct {
	Strategy EvictStrategy
	KeyLimit *int
}

type EvictionManager struct {
	strategy EvictStrategy
	keyLimit *int
	keys     map[string]struct{}
	order    *list.List
	elements map[string]*list.Element
}

func NewEvictionManager(config EvictionConfig) *EvictionManager {
	return &EvictionManager{
		strategy: config.Strategy,
		keyLimit: config.KeyLimit,
		keys:     make(map[string]struct{}),
		order:    list.New(),
		elements: make(map[string]*list.Element),
	}
}

func (manager *EvictionManager) IsEnabled() bool {
	return manager.keyLimit != nil && *manager.keyLimit > 0 && manager.strategy == EvictLRU
}

func (manager *EvictionManager) RecordInsert(key string) {
	if !manager.IsEnabled() {
		return
	}
	if _, exists := manager.keys[key]; exists {
		manager.order.MoveToFront(manager.elements[key])
		return
	}
	manager.keys[key] = struct{}{}
	manager.elements[key] = manager.order.PushFront(key)
}

func (manager *EvictionManager) RecordAccess(key string) {
	if !manager.IsEnabled() {
		return
	}
	if element := manager.elements[key]; element != nil {
		manager.order.MoveToFront(element)
	}
}

func (manager *EvictionManager) RecordDelete(key string) {
	if !manager.IsEnabled() {
		return
	}
	manager.remove(key)
}

func (manager *EvictionManager) ShouldEvict() bool {
	if !manager.IsEnabled() {
		return false
	}
	return len(manager.keys) > *manager.keyLimit
}

func (manager *EvictionManager) NextEviction() string {
	if !manager.IsEnabled() {
		return ""
	}

	if len(manager.keys) <= *manager.keyLimit {
		return ""
	}
	element := manager.order.Back()
	if element == nil {
		return ""
	}
	key := element.Value.(string)
	return key
}

func (manager *EvictionManager) remove(key string) {
	delete(manager.keys, key)
	if element := manager.elements[key]; element != nil {
		manager.order.Remove(element)
		delete(manager.elements, key)
	}
}

func (manager *EvictionManager) KeyCount() int {
	return len(manager.keys)
}

func (manager *EvictionManager) Reset() {
	manager.keys = make(map[string]struct{})
	manager.order.Init()
	manager.elements = make(map[string]*list.Element)
}
