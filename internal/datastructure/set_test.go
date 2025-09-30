package datastructure

import (
	"testing"
	"time"
)

func TestSetSadd(t *testing.T) {
	s := CreateSet()
	added := s.Sadd("myset", "m1", "m2", "m3")
	if added != 3 {
		t.Errorf("Expected 3 added, got %d", added)
	}

	added = s.Sadd("myset", "m2", "m4")
	if added != 1 {
		t.Errorf("Expected 1 added (m4), got %d", added)
	}
}

func TestSetSmembers(t *testing.T) {
	s := CreateSet()
	s.Sadd("myset", "a", "b", "c")

	members, ok := s.Smembers("myset")
	if !ok || len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	_, ok = s.Smembers("nonexistent")
	if ok {
		t.Error("Expected set not found")
	}
}

func TestSetSismember(t *testing.T) {
	s := CreateSet()
	s.Sadd("myset", "a", "b")

	if !s.Sismember("myset", "a") {
		t.Error("a should be member")
	}
	if s.Sismember("myset", "c") {
		t.Error("c should not be member")
	}
}

func TestSetSrem(t *testing.T) {
	s := CreateSet()
	s.Sadd("myset", "a", "b", "c")

	removed := s.Srem("myset", "b", "d")
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}

	if s.Sismember("myset", "b") {
		t.Error("b should be removed")
	}
	if !s.Sismember("myset", "a") {
		t.Error("a should still exist")
	}
}

func TestSetScard(t *testing.T) {
	s := CreateSet()
	s.Sadd("myset", "a", "b", "c")

	count := s.Scard("myset")
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}

	count = s.Scard("nonexistent")
	if count != 0 {
		t.Errorf("Expected 0, got %d", count)
	}
}

func TestSetExpire(t *testing.T) {
	s := CreateSet()
	s.Sadd("myset", "a", "b")

	ok := s.Expire("myset", 100*time.Millisecond)
	if !ok {
		t.Error("Expire should succeed")
	}

	time.Sleep(150 * time.Millisecond)

	count := s.Scard("myset")
	if count != 0 {
		t.Error("Set should be expired")
	}
}
