package command

import (
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func setupHashContext() {
	SetHashContext(&HashContext{
		Hash: datastructure.CreateHashMap(),
		AOF:  nil,
	})
}

func TestCmdHset(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	}
	
	result := cmdHset(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 1 {
		t.Errorf("expected 1 (new field), got %d", result.Number)
	}
	
	result = cmdHset(args)
	if result.Number != 0 {
		t.Errorf("expected 0 (existing field), got %d", result.Number)
	}
}

func TestCmdHsetInvalidArgs(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
	}
	
	result := cmdHset(args)
	
	if result.Type != resp.Error {
		t.Errorf("expected Error type, got %v", result.Type)
	}
}

func TestCmdHget(t *testing.T) {
	setupHashContext()
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
	}
	
	result := cmdHget(args)
	
	if result.Type != resp.BulkString {
		t.Errorf("expected BulkString type, got %v", result.Type)
	}
	if result.Text != "John" {
		t.Errorf("expected 'John', got %s", result.Text)
	}
}

func TestCmdHgetNonexistent(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
	}
	
	result := cmdHget(args)
	
	if result.Type != resp.BulkString {
		t.Errorf("expected BulkString type, got %v", result.Type)
	}
	if !result.IsNil {
		t.Error("expected nil result")
	}
}

func TestCmdHdel(t *testing.T) {
	setupHashContext()
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "30"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
	}
	
	result := cmdHdel(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 1 {
		t.Errorf("expected 1, got %d", result.Number)
	}
	
	getResult := cmdHget([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
	})
	
	if !getResult.IsNil {
		t.Error("expected field to be deleted")
	}
}

func TestCmdHdelMultiple(t *testing.T) {
	setupHashContext()
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "30"},
	})
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "city"},
		{Type: resp.BulkString, Text: "NYC"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "city"},
	}
	
	result := cmdHdel(args)
	
	if result.Number != 2 {
		t.Errorf("expected 2, got %d", result.Number)
	}
}

func TestCmdHgetall(t *testing.T) {
	setupHashContext()
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "30"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
	}
	
	result := cmdHgetall(args)
	
	if result.Type != resp.Array {
		t.Errorf("expected Array type, got %v", result.Type)
	}
	if len(result.Items) != 4 {
		t.Errorf("expected 4 items (2 field-value pairs), got %d", len(result.Items))
	}
}

func TestCmdHgetallNonexistent(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "nonexistent"},
	}
	
	result := cmdHgetall(args)
	
	if result.Type != resp.Array {
		t.Errorf("expected Array type, got %v", result.Type)
	}
	if len(result.Items) != 0 {
		t.Errorf("expected empty array, got %d items", len(result.Items))
	}
}

func TestCmdHexists(t *testing.T) {
	setupHashContext()
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
	}
	
	result := cmdHexists(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 1 {
		t.Errorf("expected 1, got %d", result.Number)
	}
	
	args = []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
	}
	
	result = cmdHexists(args)
	
	if result.Number != 0 {
		t.Errorf("expected 0, got %d", result.Number)
	}
}

func TestCmdHlen(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
	}
	
	result := cmdHlen(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 0 {
		t.Errorf("expected 0, got %d", result.Number)
	}
	
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
	})
	cmdHset([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "30"},
	})
	
	result = cmdHlen(args)
	
	if result.Number != 2 {
		t.Errorf("expected 2, got %d", result.Number)
	}
}

func TestCmdHlenInvalidArgs(t *testing.T) {
	setupHashContext()
	
	result := cmdHlen([]resp.Value{})
	
	if result.Type != resp.Error {
		t.Errorf("expected Error type, got %v", result.Type)
	}
}

func TestCmdHsetMultiple(t *testing.T) {
	setupHashContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
		{Type: resp.BulkString, Text: "John"},
		{Type: resp.BulkString, Text: "age"},
		{Type: resp.BulkString, Text: "30"},
		{Type: resp.BulkString, Text: "city"},
		{Type: resp.BulkString, Text: "NYC"},
	}
	
	result := cmdHset(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 3 {
		t.Errorf("expected 3 new fields, got %d", result.Number)
	}
	
	nameResult := cmdHget([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "name"},
	})
	if nameResult.Text != "John" {
		t.Errorf("expected 'John', got %s", nameResult.Text)
	}
	
	ageResult := cmdHget([]resp.Value{
		{Type: resp.BulkString, Text: "user:1"},
		{Type: resp.BulkString, Text: "age"},
	})
	if ageResult.Text != "30" {
		t.Errorf("expected '30', got %s", ageResult.Text)
	}
}
