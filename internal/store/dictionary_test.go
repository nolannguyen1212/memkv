package store

import "testing"

func TestDictionarySetGetAndDelete(t *testing.T) {
	database := New(Config{})
	dictionary := database.Dictionary
	dictionary.Set("key", "value")

	value, exists := dictionary.Get("key")
	if !exists || value != "value" {
		t.Fatalf("Get() = %q, %v", value, exists)
	}
	if deleted := database.DeleteKey("key"); !deleted {
		t.Fatal("DeleteKey() = false, want true")
	}
	if _, exists := dictionary.Get("key"); exists {
		t.Fatal("deleted key still exists")
	}
}

func TestDictionarySnapshotIsIndependent(t *testing.T) {
	dictionary := New(Config{}).Dictionary
	dictionary.Set("key", "value")
	snapshot := dictionary.Snapshot()
	dictionary.Set("key", "updated")

	if snapshot["key"].Value != "value" {
		t.Fatalf("snapshot changed to %q", snapshot["key"].Value)
	}
}
