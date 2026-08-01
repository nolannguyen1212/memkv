package store

import "testing"

func TestEvictionLRU(t *testing.T) {
	limit := 3
	var evicted string
	em := NewEvictionManager(EvictionConfig{
		Strategy: EvictLRU,
		KeyLimit: &limit,
		OnEvict:  func(k string) { evicted = k },
	})

	em.RecordInsert("a")
	em.RecordInsert("b")
	em.RecordInsert("c")
	em.RecordInsert("d")

	em.EvictOne()
	if evicted != "a" {
		t.Errorf("Expected 'a' evicted, got '%s'", evicted)
	}
}

func TestEvictionNone(t *testing.T) {
	limit := 2
	em := NewEvictionManager(EvictionConfig{
		Strategy: EvictNone,
		KeyLimit: &limit,
	})

	em.RecordInsert("a")
	em.RecordInsert("b")
	em.RecordInsert("c")

	if em.ShouldEvict() {
		t.Error("Should not evict when strategy is none")
	}
}

func TestEvictionNoLimit(t *testing.T) {
	em := NewEvictionManager(EvictionConfig{
		Strategy: EvictLRU,
		KeyLimit: nil,
	})

	em.RecordInsert("a")
	em.RecordInsert("b")

	if em.ShouldEvict() {
		t.Error("Should not evict when no limit")
	}
}
