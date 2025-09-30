package command

import (
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func setupSetTest() *datastructure.Set {
	set := datastructure.CreateSet()
	SetSetContext(&SetContext{Set: set, AOF: nil})
	return set
}

func TestCmdSAdd(t *testing.T) {
	set := setupSetTest()

	result := cmdSAdd([]resp.Value{
		{Type: resp.BulkString, Text: "myset"},
		{Type: resp.BulkString, Text: "m1"},
		{Type: resp.BulkString, Text: "m2"},
	})

	if result.Number != 2 {
		t.Errorf("Expected 2 added, got %d", result.Number)
	}

	if !set.Sismember("myset", "m1") {
		t.Error("m1 should be member")
	}
}

func TestCmdSMembers(t *testing.T) {
	set := setupSetTest()
	set.Sadd("myset", "a", "b", "c")

	result := cmdSMembers([]resp.Value{{Type: resp.BulkString, Text: "myset"}})

	if result.Type != resp.Array || len(result.Items) != 3 {
		t.Errorf("Expected 3 members, got %d", len(result.Items))
	}
}

func TestCmdSIsMember(t *testing.T) {
	set := setupSetTest()
	set.Sadd("myset", "a", "b")

	result := cmdSIsMember([]resp.Value{
		{Type: resp.BulkString, Text: "myset"},
		{Type: resp.BulkString, Text: "a"},
	})
	if result.Number != 1 {
		t.Error("Expected 1 (member exists)")
	}

	result = cmdSIsMember([]resp.Value{
		{Type: resp.BulkString, Text: "myset"},
		{Type: resp.BulkString, Text: "c"},
	})
	if result.Number != 0 {
		t.Error("Expected 0 (member not exists)")
	}
}

func TestCmdSRem(t *testing.T) {
	set := setupSetTest()
	set.Sadd("myset", "a", "b", "c")

	result := cmdSRem([]resp.Value{
		{Type: resp.BulkString, Text: "myset"},
		{Type: resp.BulkString, Text: "b"},
		{Type: resp.BulkString, Text: "d"},
	})

	if result.Number != 1 {
		t.Errorf("Expected 1 removed, got %d", result.Number)
	}

	if set.Sismember("myset", "b") {
		t.Error("b should be removed")
	}
}

func TestCmdSCard(t *testing.T) {
	set := setupSetTest()
	set.Sadd("myset", "a", "b", "c")

	result := cmdSCard([]resp.Value{{Type: resp.BulkString, Text: "myset"}})

	if result.Number != 3 {
		t.Errorf("Expected 3, got %d", result.Number)
	}
}
