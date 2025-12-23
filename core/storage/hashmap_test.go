package storage

import "testing"

func TestHashMapSetGet(t *testing.T) {
	hashMap := NewHashMap()

	count := hashMap.Set("myhash", "field1", "value1", "field2", "value2")
	if count != 2 {
		t.Errorf("Expected 2 added, got %d", count)
	}

	value, exists := hashMap.Get("myhash", "field1")
	if !exists || value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}
}

func TestHashMapDelete(t *testing.T) {
	hashMap := NewHashMap()
	hashMap.Set("myhash", "field1", "value1", "field2", "value2")

	count := hashMap.Delete("myhash", "field1", "field3")
	if count != 1 {
		t.Errorf("Expected 1 deleted, got %d", count)
	}
}

func TestHashMapGetAll(t *testing.T) {
	hashMap := NewHashMap()
	hashMap.Set("myhash", "f1", "v1", "f2", "v2")

	all, exists := hashMap.GetAll("myhash")
	if !exists || len(all) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(all))
	}
}

func TestHashMapFieldCount(t *testing.T) {
	hashMap := NewHashMap()
	hashMap.Set("myhash", "f1", "v1", "f2", "v2", "f3", "v3")

	if hashMap.FieldCount("myhash") != 3 {
		t.Error("Expected 3 fields")
	}
}
