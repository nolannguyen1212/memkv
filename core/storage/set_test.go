package storage

import (
	"testing"
	"time"
)

func TestSetAddMembers(t *testing.T) {
	set := NewSet(DefaultExpirationConfig)

	count := set.Add("myset", "a", "b", "c")
	if count != 3 {
		t.Errorf("Expected 3 added, got %d", count)
	}

	count = set.Add("myset", "a", "d")
	if count != 1 {
		t.Errorf("Expected 1 added (d only), got %d", count)
	}
}

func TestSetRemoveMembers(t *testing.T) {
	set := NewSet(DefaultExpirationConfig)
	set.Add("myset", "a", "b", "c")

	count := set.Remove("myset", "a", "x")
	if count != 1 {
		t.Errorf("Expected 1 removed, got %d", count)
	}
}

func TestSetMembers(t *testing.T) {
	set := NewSet(DefaultExpirationConfig)
	set.Add("myset", "a", "b")

	members, exists := set.Members("myset")
	if !exists || len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}
}

func TestSetCardinality(t *testing.T) {
	set := NewSet(DefaultExpirationConfig)
	set.Add("myset", "a", "b", "c")

	if set.Cardinality("myset") != 3 {
		t.Error("Expected cardinality 3")
	}
}

func TestSetExpiration(t *testing.T) {
	set := NewSet(DefaultExpirationConfig)
	set.Add("myset", "a", "b")
	set.Expire("myset", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	_, exists := set.Members("myset")
	if exists {
		t.Error("Set should be expired")
	}
}
