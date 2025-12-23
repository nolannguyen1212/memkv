package storage

import (
	"testing"
	"time"
)

func TestDictionarySetGet(t *testing.T) {
	dictionary := NewDictionary(DefaultExpirationConfig)
	dictionary.Set("key1", "value1", 0)

	value, exists := dictionary.Get("key1")
	if !exists || value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	_, exists = dictionary.Get("nonexistent")
	if exists {
		t.Error("Expected key not found")
	}
}

func TestDictionaryExpire(t *testing.T) {
	dictionary := NewDictionary(DefaultExpirationConfig)
	dictionary.Set("key1", "value1", 100*time.Millisecond)

	value, exists := dictionary.Get("key1")
	if !exists || value != "value1" {
		t.Error("Key should exist")
	}

	time.Sleep(150 * time.Millisecond)

	_, exists = dictionary.Get("key1")
	if exists {
		t.Error("Key should be expired")
	}
}

func TestDictionaryDelete(t *testing.T) {
	dictionary := NewDictionary(DefaultExpirationConfig)
	dictionary.Set("key1", "value1", 0)
	dictionary.Set("key2", "value2", 0)

	count := dictionary.Delete("key1", "key3")
	if count != 1 {
		t.Errorf("Expected 1 deleted, got %d", count)
	}

	_, exists := dictionary.Get("key1")
	if exists {
		t.Error("key1 should be deleted")
	}

	value, exists := dictionary.Get("key2")
	if !exists || value != "value2" {
		t.Error("key2 should still exist")
	}
}

func TestDictionaryTTL(t *testing.T) {
	dictionary := NewDictionary(DefaultExpirationConfig)
	dictionary.Set("key1", "value1", 0)
	dictionary.Set("key2", "value2", 10*time.Second)

	ttl := dictionary.TTL("key1")
	if ttl != TTLNoExpire {
		t.Errorf("Expected %d (no expiry), got %d", TTLNoExpire, ttl)
	}

	ttl = dictionary.TTL("key2")
	if ttl < 9 || ttl > 10 {
		t.Errorf("Expected ~10s, got %d", ttl)
	}

	ttl = dictionary.TTL("nonexistent")
	if ttl != TTLNotFound {
		t.Errorf("Expected %d (not found), got %d", TTLNotFound, ttl)
	}
}

func TestDictionarySnapshot(t *testing.T) {
	dictionary := NewDictionary(DefaultExpirationConfig)
	dictionary.Set("key1", "value1", 0)
	dictionary.Set("key2", "value2", 0)
	dictionary.Set("expired", "val", 1*time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	snapshot := dictionary.Snapshot()
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
