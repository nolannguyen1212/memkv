package persistence

import (
	"os"
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
)

func TestRDBSaveLoad(t *testing.T) {
	tmpFile := "test_dump.rdb"
	defer os.Remove(tmpFile)

	rdb, err := OpenRDB(tmpFile, true)
	if err != nil {
		t.Fatalf("OpenRDB failed: %v", err)
	}
	defer rdb.Close()

	snapshot := Snapshot{
		DictData: map[string]datastructure.Item{
			"key1": {Value: "value1"},
			"key2": {Value: "value2", ExpiredAt: time.Now().Add(time.Hour)},
		},
		SetData: map[string]datastructure.Item{
			"set1": {Members: map[string]struct{}{"m1": {}, "m2": {}}},
		},
	}

	if err := rdb.Save(snapshot, tmpFile); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := rdb.Load(tmpFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("Loaded snapshot is nil")
	}

	if len(loaded.DictData) != 2 {
		t.Errorf("Expected 2 dict keys, got %d", len(loaded.DictData))
	}
	if loaded.DictData["key1"].Value != "value1" {
		t.Error("key1 value mismatch")
	}

	if len(loaded.SetData) != 1 {
		t.Errorf("Expected 1 set key, got %d", len(loaded.SetData))
	}
	if len(loaded.SetData["set1"].Members) != 2 {
		t.Error("set1 members count mismatch")
	}
}

func TestRDBLoadEmpty(t *testing.T) {
	tmpFile := "test_empty.rdb"
	os.WriteFile(tmpFile, []byte{}, 0644)
	defer os.Remove(tmpFile)

	rdb, _ := OpenRDB(tmpFile, true)
	defer rdb.Close()

	snapshot, err := rdb.Load(tmpFile)
	if err != nil {
		t.Errorf("Load empty file should not error: %v", err)
	}
	if snapshot != nil {
		t.Error("Empty file should return nil snapshot")
	}
}

func TestRDBDisabled(t *testing.T) {
	rdb, _ := OpenRDB("dummy.rdb", false)

	snapshot := Snapshot{DictData: map[string]datastructure.Item{"k": {Value: "v"}}}
	if err := rdb.Save(snapshot, "dummy.rdb"); err != nil {
		t.Error("Save with disabled RDB should not error")
	}

	loaded, err := rdb.Load("dummy.rdb")
	if err != nil || loaded != nil {
		t.Error("Load with disabled RDB should return nil")
	}
}
