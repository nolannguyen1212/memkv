package persistence

import (
	"os"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func TestAOFAppendLoad(t *testing.T) {
	tmpFile := "test_append.aof"
	defer os.Remove(tmpFile)

	aof, err := OpenAOF(tmpFile, true)
	if err != nil {
		t.Fatalf("OpenAOF failed: %v", err)
	}
	defer aof.Close()

	cmd1 := resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "SET"},
			{Type: resp.BulkString, Text: "key1"},
			{Type: resp.BulkString, Text: "value1"},
		},
	}
	if err := aof.Append(cmd1); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	cmd2 := resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "SADD"},
			{Type: resp.BulkString, Text: "set1"},
			{Type: resp.BulkString, Text: "m1"},
		},
	}
	if err := aof.Append(cmd2); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	commands := []string{}
	aof.Load(tmpFile, func(cmd string, args []resp.Value) {
		commands = append(commands, cmd)
	})

	if len(commands) != 2 {
		t.Errorf("Expected 2 commands, got %d", len(commands))
	}
	if commands[0] != "SET" || commands[1] != "SADD" {
		t.Errorf("Commands mismatch: %v", commands)
	}
}

func TestAOFRewrite(t *testing.T) {
	tmpFile := "test_rewrite.aof"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + ".tmp")

	aof, _ := OpenAOF(tmpFile, true)
	defer aof.Close()

	snapshot := map[string]datastructure.Item{
		"key1": {Value: "value1"},
		"set1": {Members: map[string]struct{}{"m1": {}, "m2": {}}},
	}

	err := aof.Rewrite(func() map[string]datastructure.Item {
		return snapshot
	}, tmpFile)

	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	commands := []string{}
	aof.Load(tmpFile, func(cmd string, args []resp.Value) {
		commands = append(commands, cmd)
	})

	if len(commands) < 2 {
		t.Errorf("Expected at least 2 commands after rewrite, got %d", len(commands))
	}
}

func TestAOFDisabled(t *testing.T) {
	aof, _ := OpenAOF("dummy.aof", false)

	cmd := resp.Value{Type: resp.Array, Items: []resp.Value{{Type: resp.BulkString, Text: "PING"}}}
	if err := aof.Append(cmd); err != nil {
		t.Error("Append with disabled AOF should not error")
	}

	err := aof.Load("dummy.aof", func(cmd string, args []resp.Value) {})
	if err != nil {
		t.Error("Load with disabled AOF should not error")
	}
}
