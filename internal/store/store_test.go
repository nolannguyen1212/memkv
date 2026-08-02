package store

import (
	"fmt"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

func TestExpirationUsesInjectedClock(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	database := New(Config{Clock: clock, ExpirationCheckInterval: time.Second})
	database.SetString("key", "value", clock.now.Add(2*time.Second))

	if ttl := database.Keyspace.TTL("key"); ttl != 2 {
		t.Fatalf("TTL() = %d, want 2", ttl)
	}

	clock.now = clock.now.Add(3 * time.Second)

	if _, exists := database.Dictionary.Get("key"); exists {
		t.Fatal("expired key remained visible")
	}
}

func TestTypeReplacementRemovesPreviousPayload(t *testing.T) {
	database := New(Config{})
	database.Set.Add("key", "member")
	database.SetString("key", "value", time.Time{})

	state := database.Snapshot()

	if _, exists := state.Sets["key"]; exists {
		t.Fatal("set payload survived type replacement")
	}

	if state.Strings["key"].Value != "value" {
		t.Fatal("string payload missing after type replacement")
	}

	if state.Keyspace["key"].Type != KeyTypeString {
		t.Fatalf("type = %q, want %q", state.Keyspace["key"].Type, KeyTypeString)
	}
}

func TestMaintenanceExpiresUnifiedEntry(t *testing.T) {
	database := New(Config{ExpirationCheckInterval: time.Millisecond})
	database.SetString("key", "value", time.Time{})
	database.Keyspace.ExpireAt("key", time.Now().Add(-time.Millisecond))
	database.Maintain(time.Now().Add(time.Millisecond))

	state := database.Snapshot()

	if _, exists := state.Keyspace["key"]; exists {
		t.Fatal("expired metadata survived maintenance")
	}

	if _, exists := state.Strings["key"]; exists {
		t.Fatal("expired payload survived maintenance")
	}
}

func TestRestoreIsAtomicAndCopiesPayloads(t *testing.T) {
	database := New(Config{})
	database.SetString("old", "value", time.Time{})

	invalid := LogicalSnapshot{
		Keyspace: map[string]KeyMetadata{"broken": {Type: KeyTypeSet}},
	}

	if err := database.Restore(invalid, time.Now()); err == nil {
		t.Fatal("Restore() accepted missing payload")
	}

	if value, exists := database.Dictionary.Get("old"); !exists || value != "value" {
		t.Fatal("failed restore changed existing state")
	}

	members := map[string]struct{}{"member": {}}
	valid := LogicalSnapshot{
		Keyspace: map[string]KeyMetadata{"set": {Type: KeyTypeSet, Version: 9}},
		Sets:     map[string]SetEntry{"set": {Members: members}},
	}

	if err := database.Restore(valid, time.Now()); err != nil {
		t.Fatal(err)
	}

	delete(members, "member")

	if !database.Set.IsMember("set", "member") {
		t.Fatal("restored state aliases snapshot payload")
	}

	if database.Keyspace.Version("set") != 9 {
		t.Fatalf("version = %d, want 9", database.Keyspace.Version("set"))
	}
}

func TestVersionRemainsMonotonicAcrossDeleteAndRecreate(t *testing.T) {
	database := New(Config{})
	database.SetString("key", "first", time.Time{})

	if version := database.Keyspace.Version("key"); version != 1 {
		t.Fatalf("initial version = %d, want 1", version)
	}

	if !database.DeleteKey("key") {
		t.Fatal("DeleteKey() = false, want true")
	}

	if version := database.Keyspace.Version("key"); version != 2 {
		t.Fatalf("deleted version = %d, want 2", version)
	}

	database.SetString("key", "second", time.Time{})

	if version := database.Keyspace.Version("key"); version != 3 {
		t.Fatalf("recreated version = %d, want 3", version)
	}
}

func TestCollectionsMaintainMutationInvariants(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Store)
	}{
		{name: "dictionary", mutate: func(database *Store) {
			database.Dictionary.Set("key", "value")
		}},
		{name: "set", mutate: func(database *Store) {
			database.Set.Add("key", "member")
		}},
		{name: "list", mutate: func(database *Store) {
			database.List.RightPush("key", "value")
		}},
		{name: "hash", mutate: func(database *Store) {
			database.Hash.Set("key", "field", "value")
		}},
		{name: "sorted set", mutate: func(database *Store) {
			database.SortedSet.Add("key", ScoreMember{Score: 1, Member: "member"})
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			maxKeys := 10
			database := New(Config{MaxKeys: &maxKeys})
			mutatedKey := ""
			database.SetMutationHandler(func(key string) {
				mutatedKey = key
			})

			test.mutate(database)

			if database.Keyspace.Version("key") != 1 {
				t.Fatalf("version = %d, want 1", database.Keyspace.Version("key"))
			}

			if database.Capacity.Count() != 1 {
				t.Fatalf("capacity count = %d, want 1", database.Capacity.Count())
			}

			if mutatedKey != "key" {
				t.Fatalf("mutated key = %q, want key", mutatedKey)
			}
		})
	}
}

func TestCollectionNoOpDoesNotCreateMutation(t *testing.T) {
	database := New(Config{})
	database.Set.Add("key", "member")
	version := database.Keyspace.Version("key")
	mutations := 0
	database.SetMutationHandler(func(string) {
		mutations++
	})

	database.Set.Add("key", "member")

	if database.Keyspace.Version("key") != version {
		t.Fatal("duplicate member changed version")
	}

	if mutations != 0 {
		t.Fatalf("mutation callbacks = %d, want 0", mutations)
	}
}

func TestMaintenanceBoundsExpirationWork(t *testing.T) {
	database := New(Config{ExpirationCheckInterval: time.Millisecond})

	for index := range expirationWorkLimit + 1 {
		database.SetString(fmt.Sprintf("key-%d", index), "value", time.UnixMilli(1))
	}

	database.Maintain(time.Now().Add(time.Millisecond))

	if len(database.entries) != 1 {
		t.Fatalf("remaining entries = %d, want 1", len(database.entries))
	}
}

func BenchmarkStoreSnapshot(b *testing.B) {
	database := New(Config{})

	for index := range 10_000 {
		database.SetString(fmt.Sprintf("key-%d", index), "value", time.Time{})
	}

	b.ResetTimer()

	for b.Loop() {
		_ = database.Snapshot()
	}
}

func BenchmarkStoreString(b *testing.B) {
	database := New(Config{})
	database.SetString("key", "value", time.Time{})
	b.Run("get", func(b *testing.B) {
		for b.Loop() {
			_, _ = database.Dictionary.Get("key")
		}
	})
	b.Run("set", func(b *testing.B) {
		for b.Loop() {
			database.SetString("key", "value", time.Time{})
		}
	})
}

func BenchmarkDeque(b *testing.B) {
	deque := NewDeque[int]()
	b.Run("push-pop", func(b *testing.B) {
		for b.Loop() {
			deque.PushBack(1)
			_, _ = deque.PopFront()
		}
	})
	b.Run("growth", func(b *testing.B) {
		for b.Loop() {
			candidate := NewDeque[int]()

			for index := range 1_024 {
				candidate.PushBack(index)
			}
		}
	})
}
