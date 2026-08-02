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
	if !database.PrepareWrite("key", KeyTypeString, false) {
		t.Fatal("PrepareWrite string failed")
	}
	database.Dictionary.Set("key", "value")
	if !database.PrepareWrite("key", KeyTypeSet, true) {
		t.Fatal("PrepareWrite set failed")
	}
	database.Set.Add("key", "member")

	state := database.Snapshot()
	if _, exists := state.Strings["key"]; exists {
		t.Fatal("string payload survived type replacement")
	}
	if _, exists := state.Sets["key"].Members["member"]; !exists {
		t.Fatal("set payload missing after type replacement")
	}
	if state.Keyspace["key"].Type != KeyTypeSet {
		t.Fatalf("type = %q, want %q", state.Keyspace["key"].Type, KeyTypeSet)
	}
}

func TestMaintenanceExpiresUnifiedEntry(t *testing.T) {
	database := New(Config{ExpirationCheckInterval: time.Millisecond})
	database.PrepareWrite("key", KeyTypeString, false)
	database.Dictionary.Set("key", "value")
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
	database.PrepareWrite("old", KeyTypeString, false)
	database.Dictionary.Set("old", "value")

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

func TestMaintenanceBoundsExpirationWork(t *testing.T) {
	database := New(Config{ExpirationCheckInterval: time.Millisecond})
	for index := 0; index < expirationWorkLimit+1; index++ {
		database.SetString(fmt.Sprintf("key-%d", index), "value", time.UnixMilli(1))
	}
	database.Maintain(time.Now().Add(time.Millisecond))
	if len(database.entries) != 1 {
		t.Fatalf("remaining entries = %d, want 1", len(database.entries))
	}
}
