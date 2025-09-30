package datastructure

import (
	"testing"
	"time"
)

func TestDictSetGet(t *testing.T) {
	d := CreateDict()
	d.Set("key1", "value1", 0)

	val, ok := d.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	_, ok = d.Get("nonexistent")
	if ok {
		t.Error("Expected key not found")
	}
}

func TestDictExpire(t *testing.T) {
	d := CreateDict()
	d.Set("key1", "value1", 100*time.Millisecond)

	val, ok := d.Get("key1")
	if !ok || val != "value1" {
		t.Error("Key should exist")
	}

	time.Sleep(150 * time.Millisecond)

	_, ok = d.Get("key1")
	if ok {
		t.Error("Key should be expired")
	}
}

func TestDictDelete(t *testing.T) {
	d := CreateDict()
	d.Set("key1", "value1", 0)
	d.Set("key2", "value2", 0)

	count := d.Delete("key1", "key3")
	if count != 1 {
		t.Errorf("Expected 1 deleted, got %d", count)
	}

	_, ok := d.Get("key1")
	if ok {
		t.Error("key1 should be deleted")
	}

	val, ok := d.Get("key2")
	if !ok || val != "value2" {
		t.Error("key2 should still exist")
	}
}

func TestDictTTL(t *testing.T) {
	d := CreateDict()
	d.Set("key1", "value1", 0)
	d.Set("key2", "value2", 10*time.Second)

	ttl := d.TTL("key1")
	if ttl != -1 {
		t.Errorf("Expected -1 (no expiry), got %d", ttl)
	}

	ttl = d.TTL("key2")
	if ttl < 9 || ttl > 10 {
		t.Errorf("Expected ~10s, got %d", ttl)
	}

	ttl = d.TTL("nonexistent")
	if ttl != -2 {
		t.Errorf("Expected -2 (not found), got %d", ttl)
	}
}

func TestDictDump(t *testing.T) {
	d := CreateDict()
	d.Set("key1", "value1", 0)
	d.Set("key2", "value2", 0)
	d.Set("expired", "val", 1*time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	snapshot := d.Dump()
	if len(snapshot) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(snapshot))
	}
	if snapshot["key1"].Value != "value1" {
		t.Error("key1 value mismatch")
	}
	if _, exists := snapshot["expired"]; exists {
		t.Error("Expired key should not be in snapshot")
	}
}
