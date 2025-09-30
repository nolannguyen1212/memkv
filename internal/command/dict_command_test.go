package command

import (
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func setupDictTest() *datastructure.Dict {
	dict := datastructure.CreateDict()
	SetDictContext(&DictContext{Dict: dict, AOF: nil})
	return dict
}

func TestCmdSet(t *testing.T) {
	dict := setupDictTest()

	result := cmdSet([]resp.Value{
		{Type: resp.BulkString, Text: "key1"},
		{Type: resp.BulkString, Text: "value1"},
	})

	if result.Type != resp.SimpleString || result.Text != "OK" {
		t.Errorf("Expected OK, got %v", result)
	}

	val, ok := dict.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestCmdGet(t *testing.T) {
	dict := setupDictTest()
	dict.Set("key1", "value1", 0)

	result := cmdGet([]resp.Value{{Type: resp.BulkString, Text: "key1"}})
	if result.Type != resp.BulkString || result.Text != "value1" {
		t.Errorf("Expected value1, got %v", result)
	}

	result = cmdGet([]resp.Value{{Type: resp.BulkString, Text: "nonexistent"}})
	if !result.IsNil {
		t.Error("Expected nil for nonexistent key")
	}
}

func TestCmdDel(t *testing.T) {
	dict := setupDictTest()
	dict.Set("key1", "value1", 0)
	dict.Set("key2", "value2", 0)

	result := cmdDel([]resp.Value{
		{Type: resp.BulkString, Text: "key1"},
		{Type: resp.BulkString, Text: "key3"},
	})

	if result.Number != 1 {
		t.Errorf("Expected 1 deleted, got %d", result.Number)
	}

	_, ok := dict.Get("key1")
	if ok {
		t.Error("key1 should be deleted")
	}
}

func TestCmdTTL(t *testing.T) {
	dict := setupDictTest()
	dict.Set("key1", "value1", 0)
	dict.Set("key2", "value2", 10*time.Second)

	result := cmdTTL([]resp.Value{{Type: resp.BulkString, Text: "key1"}})
	if result.Number != -1 {
		t.Errorf("Expected -1, got %d", result.Number)
	}

	result = cmdTTL([]resp.Value{{Type: resp.BulkString, Text: "key2"}})
	if result.Number < 9 || result.Number > 10 {
		t.Errorf("Expected ~10, got %d", result.Number)
	}
}

func TestCmdExpire(t *testing.T) {
	dict := setupDictTest()
	dict.Set("key1", "value1", 0)

	result := cmdExpire([]resp.Value{
		{Type: resp.BulkString, Text: "key1"},
		{Type: resp.BulkString, Text: "5"},
	})

	if result.Number != 1 {
		t.Errorf("Expected 1, got %d", result.Number)
	}

	ttl := dict.TTL("key1")
	if ttl < 4 || ttl > 5 {
		t.Errorf("Expected ~5s TTL, got %d", ttl)
	}
}

func TestCmdPing(t *testing.T) {
	result := cmdPing([]resp.Value{})
	if result.Type != resp.SimpleString || result.Text != "PONG" {
		t.Errorf("Expected PONG, got %v", result)
	}

	result = cmdPing([]resp.Value{{Type: resp.BulkString, Text: "hello"}})
	if result.Text != "hello" {
		t.Errorf("Expected hello, got %s", result.Text)
	}
}
