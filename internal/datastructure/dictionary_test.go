package datastructure

import (
	"testing"
	"time"
)

func TestDictionarySetGet(t *testing.T) {
	d := NewDictionary(DefaultExpirationConfig)
	d.Set("k1", "v1", 0)

	v, ok := d.Get("k1")

	if !ok || v != "v1" {
		t.Errorf("Expected v1, got %s", v)
	}
}

func TestDictionaryExpire(t *testing.T) {
	d := NewDictionary(DefaultExpirationConfig)
	d.Set("k1", "v1", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	_, ok := d.Get("k1")
	if ok {
		t.Error("Key should be expired")
	}
}

func TestDictionaryTTL(t *testing.T) {
	d := NewDictionary(DefaultExpirationConfig)
	d.Set("k1", "v1", 0)
	d.Set("k2", "v2", 10*time.Second)

	if d.TTL("k1") != TTLNoExpire {
		t.Error("Expected no expire")
	}
	if d.TTL("k2") < 9 {
		t.Error("Expected ~10s TTL")
	}
	if d.TTL("notexist") != TTLNotFound {
		t.Error("Expected not found")
	}
}
