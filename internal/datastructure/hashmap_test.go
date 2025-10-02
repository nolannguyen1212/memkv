package datastructure

import (
	"testing"
)

func TestHashMapHset(t *testing.T) {
	h := CreateHashMap()
	
	n := h.Hset("user:1", "name", "John")
	if n != 1 {
		t.Errorf("expected 1 (new field), got %d", n)
	}
	
	n = h.Hset("user:1", "name", "Jane")
	if n != 0 {
		t.Errorf("expected 0 (existing field), got %d", n)
	}
}

func TestHashMapHget(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "age", "30")
	
	val, ok := h.Hget("user:1", "name")
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != "John" {
		t.Errorf("expected 'John', got %s", val)
	}
	
	val, ok = h.Hget("user:1", "age")
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != "30" {
		t.Errorf("expected '30', got %s", val)
	}
}

func TestHashMapHgetNonexistent(t *testing.T) {
	h := CreateHashMap()
	
	_, ok := h.Hget("user:1", "name")
	if ok {
		t.Error("expected ok to be false for nonexistent key")
	}
	
	h.Hset("user:1", "name", "John")
	_, ok = h.Hget("user:1", "age")
	if ok {
		t.Error("expected ok to be false for nonexistent field")
	}
}

func TestHashMapHdel(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "age", "30")
	h.Hset("user:1", "city", "NYC")
	
	n := h.Hdel("user:1", "age", "city")
	if n != 2 {
		t.Errorf("expected 2 fields deleted, got %d", n)
	}
	
	_, ok := h.Hget("user:1", "age")
	if ok {
		t.Error("expected age to be deleted")
	}
	
	val, ok := h.Hget("user:1", "name")
	if !ok || val != "John" {
		t.Error("expected name to still exist")
	}
}

func TestHashMapHdelAll(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	
	h.Hdel("user:1", "name")
	
	if h.Hlen("user:1") != 0 {
		t.Error("expected hash to be removed when all fields deleted")
	}
}

func TestHashMapHgetall(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "age", "30")
	h.Hset("user:1", "city", "NYC")
	
	hash, ok := h.Hgetall("user:1")
	if !ok {
		t.Error("expected ok to be true")
	}
	
	if len(hash) != 3 {
		t.Errorf("expected 3 fields, got %d", len(hash))
	}
	
	if hash["name"] != "John" {
		t.Errorf("expected name='John', got %s", hash["name"])
	}
	if hash["age"] != "30" {
		t.Errorf("expected age='30', got %s", hash["age"])
	}
	if hash["city"] != "NYC" {
		t.Errorf("expected city='NYC', got %s", hash["city"])
	}
}

func TestHashMapHgetallNonexistent(t *testing.T) {
	h := CreateHashMap()
	
	_, ok := h.Hgetall("nonexistent")
	if ok {
		t.Error("expected ok to be false for nonexistent key")
	}
}

func TestHashMapHexists(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	
	if !h.Hexists("user:1", "name") {
		t.Error("expected field to exist")
	}
	
	if h.Hexists("user:1", "age") {
		t.Error("expected field to not exist")
	}
	
	if h.Hexists("user:2", "name") {
		t.Error("expected key to not exist")
	}
}

func TestHashMapHlen(t *testing.T) {
	h := CreateHashMap()
	
	if h.Hlen("user:1") != 0 {
		t.Error("expected length 0 for nonexistent key")
	}
	
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "age", "30")
	
	if h.Hlen("user:1") != 2 {
		t.Errorf("expected length 2, got %d", h.Hlen("user:1"))
	}
	
	h.Hdel("user:1", "age")
	
	if h.Hlen("user:1") != 1 {
		t.Errorf("expected length 1, got %d", h.Hlen("user:1"))
	}
}

func TestHashMapDump(t *testing.T) {
	h := CreateHashMap()
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "age", "30")
	h.Hset("user:2", "name", "Jane")
	
	snapshot := h.Dump()
	
	if len(snapshot) != 2 {
		t.Errorf("expected 2 hashes, got %d", len(snapshot))
	}
	
	if len(snapshot["user:1"]) != 2 {
		t.Errorf("expected user:1 to have 2 fields, got %d", len(snapshot["user:1"]))
	}
	
	if snapshot["user:1"]["name"] != "John" {
		t.Errorf("expected name='John', got %s", snapshot["user:1"]["name"])
	}
	
	if len(snapshot["user:2"]) != 1 {
		t.Errorf("expected user:2 to have 1 field, got %d", len(snapshot["user:2"]))
	}
}

func TestHashMapMultipleKeys(t *testing.T) {
	h := CreateHashMap()
	
	h.Hset("user:1", "name", "John")
	h.Hset("user:2", "name", "Jane")
	h.Hset("user:3", "name", "Bob")
	
	val1, _ := h.Hget("user:1", "name")
	val2, _ := h.Hget("user:2", "name")
	val3, _ := h.Hget("user:3", "name")
	
	if val1 != "John" || val2 != "Jane" || val3 != "Bob" {
		t.Error("values not isolated between keys")
	}
}

func TestHashMapOverwrite(t *testing.T) {
	h := CreateHashMap()
	
	h.Hset("user:1", "name", "John")
	h.Hset("user:1", "name", "Jane")
	h.Hset("user:1", "name", "Bob")
	
	val, _ := h.Hget("user:1", "name")
	if val != "Bob" {
		t.Errorf("expected 'Bob', got %s", val)
	}
	
	if h.Hlen("user:1") != 1 {
		t.Errorf("expected length 1, got %d", h.Hlen("user:1"))
	}
}

func TestHashMapConcurrency(t *testing.T) {
	h := CreateHashMap()
	done := make(chan bool)
	
	for i := 0; i < 10; i++ {
		go func(n int) {
			h.Hset("concurrent", "field", "value")
			h.Hget("concurrent", "field")
			h.Hdel("concurrent", "field")
			done <- true
		}(i)
	}
	
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestHashMapEmptyValue(t *testing.T) {
	h := CreateHashMap()
	
	h.Hset("user:1", "name", "")
	
	val, ok := h.Hget("user:1", "name")
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != "" {
		t.Errorf("expected empty string, got %s", val)
	}
}

func TestHashMapHsetMultiple(t *testing.T) {
	h := CreateHashMap()
	
	n := h.Hset("user:1", "name", "John", "age", "30", "city", "NYC")
	
	if n != 3 {
		t.Errorf("expected 3 new fields, got %d", n)
	}
	
	name, _ := h.Hget("user:1", "name")
	age, _ := h.Hget("user:1", "age")
	city, _ := h.Hget("user:1", "city")
	
	if name != "John" || age != "30" || city != "NYC" {
		t.Error("fields not set correctly")
	}
	
	n = h.Hset("user:1", "name", "Jane", "email", "jane@example.com")
	
	if n != 1 {
		t.Errorf("expected 1 new field (email), got %d", n)
	}
	
	name, _ = h.Hget("user:1", "name")
	email, _ := h.Hget("user:1", "email")
	
	if name != "Jane" || email != "jane@example.com" {
		t.Error("fields not updated correctly")
	}
}

func TestHashMapHsetOddArgs(t *testing.T) {
	h := CreateHashMap()
	
	n := h.Hset("user:1", "name")
	
	if n != 0 {
		t.Errorf("expected 0 for odd number of args, got %d", n)
	}
}
