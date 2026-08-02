package store

import "testing"

func TestEvictionLRU(t *testing.T) {
	limit := 3
	em := NewEvictionManager(EvictionConfig{
		Strategy: EvictLRU,
		KeyLimit: &limit,
	})

	em.RecordInsert("a")
	em.RecordInsert("b")
	em.RecordInsert("c")
	em.RecordInsert("d")

	if candidate := em.NextEviction(); candidate != "a" {
		t.Errorf("NextEviction() = %q, want %q", candidate, "a")
	}
	if em.KeyCount() != 4 {
		t.Fatalf("NextEviction removed candidate, key count = %d", em.KeyCount())
	}
}

func TestWritingExistingKeyRefreshesLRUOrder(t *testing.T) {
	limit := 2
	manager := NewEvictionManager(EvictionConfig{Strategy: EvictLRU, KeyLimit: &limit})
	manager.RecordInsert("a")
	manager.RecordInsert("b")
	manager.RecordInsert("a")
	manager.RecordInsert("c")
	if candidate := manager.NextEviction(); candidate != "b" {
		t.Fatalf("NextEviction() = %q, want b", candidate)
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
