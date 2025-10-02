package command

import (
	"os"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func TestCmdKeys(t *testing.T) {
	dict := datastructure.CreateDict()
	dict.Set("user:1", "alice", 0)
	dict.Set("user:2", "bob", 0)
	dict.Set("session:abc", "data", 0)

	set := datastructure.CreateSet()
	set.Sadd("myset", "m1")
	
	list := datastructure.CreateList()
	list.Lpush("mylist", "a")

	hash := datastructure.CreateHashMap()
	hash.Hset("myhash", "field", "value")

	SetSystemContext(&SystemContext{
		DB: &DB{
			Dict: dict,
			Set:  set,
			List: list,
			Hash: hash,
		},
	})

	tests := []struct {
		pattern string
		want    int
	}{
		{"*", 6},
		{"user:*", 2},
		{"user:?", 2},
		{"session:*", 1},
		{"*set", 1},
		{"nonexistent:*", 0},
		{"*list", 1},
		{"*hash", 1},
	}

	for _, tt := range tests {
		args := []resp.Value{{Type: resp.BulkString, Text: tt.pattern}}
		result := cmdKeys(args)

		if result.Type != resp.Array {
			t.Errorf("Pattern %s: expected array, got %v", tt.pattern, result.Type)
		}
		if len(result.Items) != tt.want {
			t.Errorf("Pattern %s: expected %d keys, got %d", tt.pattern, tt.want, len(result.Items))
		}
	}
}

func TestCmdKeysError(t *testing.T) {
	result := cmdKeys([]resp.Value{})
	if result.Type != resp.Error {
		t.Error("Expected error for missing argument")
	}
}

func TestCmdBgsave(t *testing.T) {
	dict := datastructure.CreateDict()
	dict.Set("key1", "value1", 0)

	set := datastructure.CreateSet()
	set.Sadd("myset", "m1")

	list := datastructure.CreateList()
	list.Lpush("mylist", "a")

	hash := datastructure.CreateHashMap()
	hash.Hset("myhash", "field", "value")	

	tmpFile := "test_bgsave.rdb"
	rdb, _ := persistence.OpenRDB(tmpFile, true)
	defer os.Remove(tmpFile)

	SetSystemContext(&SystemContext{
		DB: &DB{
			Dict: dict,
			Set:  set,
			List: list,
			Hash: hash,
			RDB:  rdb,
		},
	})

	result := cmdBgsave([]resp.Value{})
	if result.Type != resp.SimpleString {
		t.Errorf("Expected SimpleString, got %v", result.Type)
	}
	if result.Text != "Background saving started" {
		t.Errorf("Expected 'Background saving started', got %s", result.Text)
	}
}
