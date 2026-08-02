package store

import (
	"reflect"
	"testing"
)

func TestCapacityPlansFIFOEviction(t *testing.T) {
	maxKeys := 2
	capacity := NewCapacity(&maxKeys)
	capacity.Added("a")
	capacity.Added("b")

	if victims := capacity.PlanInsert("c"); !reflect.DeepEqual(victims, []string{"a"}) {
		t.Fatalf("PlanInsert(c) = %v, want [a]", victims)
	}

	if keys := capacity.Keys(); !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("Keys() = %v, want [a b]", keys)
	}
}

func TestCapacityDoesNotMoveExistingKey(t *testing.T) {
	maxKeys := 2
	capacity := NewCapacity(&maxKeys)
	capacity.Added("a")
	capacity.Added("b")
	capacity.Added("a")

	if victims := capacity.PlanInsert("c"); !reflect.DeepEqual(victims, []string{"a"}) {
		t.Fatalf("PlanInsert(c) = %v, want [a]", victims)
	}
}

func TestCapacityDeleteAndRestore(t *testing.T) {
	maxKeys := 2
	capacity := NewCapacity(&maxKeys)
	capacity.Restore([]string{"a", "b"})
	capacity.Deleted("a")

	if keys := capacity.Keys(); !reflect.DeepEqual(keys, []string{"b"}) {
		t.Fatalf("Keys() = %v, want [b]", keys)
	}
}

func TestCapacityWithoutLimitDoesNotEvict(t *testing.T) {
	capacity := NewCapacity(nil)
	capacity.Added("a")

	if victims := capacity.PlanInsert("b"); len(victims) != 0 {
		t.Fatalf("PlanInsert(b) = %v, want no victims", victims)
	}
}
