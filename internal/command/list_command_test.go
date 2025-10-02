package command

import (
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func setupListContext() {
	SetListContext(&ListContext{
		List: datastructure.CreateList(),
		AOF:  nil,
	})
}

func TestCmdLpush(t *testing.T) {
	setupListContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "a"},
		{Type: resp.BulkString, Text: "b"},
	}
	
	result := cmdLpush(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 2 {
		t.Errorf("expected 2, got %d", result.Number)
	}
}

func TestCmdLpushInvalidArgs(t *testing.T) {
	setupListContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
	}
	
	result := cmdLpush(args)
	
	if result.Type != resp.Error {
		t.Errorf("expected Error type, got %v", result.Type)
	}
}

func TestCmdRpush(t *testing.T) {
	setupListContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "a"},
		{Type: resp.BulkString, Text: "b"},
	}
	
	result := cmdRpush(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 2 {
		t.Errorf("expected 2, got %d", result.Number)
	}
}

func TestCmdLpop(t *testing.T) {
	setupListContext()
	
	cmdLpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "c"},
		{Type: resp.BulkString, Text: "b"},
		{Type: resp.BulkString, Text: "a"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "2"},
	}
	
	result := cmdLpop(args)
	
	if result.Type != resp.Array {
		t.Errorf("expected Array type, got %v", result.Type)
	}
	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0].Text != "a" {
		t.Errorf("expected 'a', got %s", result.Items[0].Text)
	}
}

func TestCmdLpopInvalidArgs(t *testing.T) {
	setupListContext()
	
	result := cmdLpop([]resp.Value{})
	
	if result.Type != resp.Error {
		t.Errorf("expected Error type, got %v", result.Type)
	}
}

func TestCmdRpop(t *testing.T) {
	setupListContext()
	
	cmdRpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "a"},
		{Type: resp.BulkString, Text: "b"},
		{Type: resp.BulkString, Text: "c"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "2"},
	}
	
	result := cmdRpop(args)
	
	if result.Type != resp.Array {
		t.Errorf("expected Array type, got %v", result.Type)
	}
	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0].Text != "c" {
		t.Errorf("expected 'c', got %s", result.Items[0].Text)
	}
}

func TestCmdLlen(t *testing.T) {
	setupListContext()
	
	cmdRpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "a"},
		{Type: resp.BulkString, Text: "b"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
	}
	
	result := cmdLlen(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 2 {
		t.Errorf("expected 2, got %d", result.Number)
	}
}

func TestCmdLlenNonexistent(t *testing.T) {
	setupListContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "nonexistent"},
	}
	
	result := cmdLlen(args)
	
	if result.Type != resp.Integer {
		t.Errorf("expected Integer type, got %v", result.Type)
	}
	if result.Number != 0 {
		t.Errorf("expected 0, got %d", result.Number)
	}
}

func TestCmdLrange(t *testing.T) {
	setupListContext()
	
	cmdRpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "a"},
		{Type: resp.BulkString, Text: "b"},
		{Type: resp.BulkString, Text: "c"},
		{Type: resp.BulkString, Text: "d"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "1"},
		{Type: resp.BulkString, Text: "2"},
	}
	
	result := cmdLrange(args)
	
	if result.Type != resp.Array {
		t.Errorf("expected Array type, got %v", result.Type)
	}
	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0].Text != "b" || result.Items[1].Text != "c" {
		t.Errorf("unexpected values: %v", result.Items)
	}
}

func TestCmdLrangeInvalidArgs(t *testing.T) {
	setupListContext()
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "invalid"},
		{Type: resp.BulkString, Text: "2"},
	}
	
	result := cmdLrange(args)
	
	if result.Type != resp.Error {
		t.Errorf("expected Error type, got %v", result.Type)
	}
}

func TestCmdSort(t *testing.T) {
	setupListContext()
	
	cmdRpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "3"},
		{Type: resp.BulkString, Text: "1"},
		{Type: resp.BulkString, Text: "2"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "ASC"},
	}
	
	result := cmdSort(args)
	
	if result.Type != resp.SimpleString {
		t.Errorf("expected SimpleString type, got %v", result.Type)
	}
	if result.Text != "OK" {
		t.Errorf("expected 'OK', got %s", result.Text)
	}
	
	rangeResult := cmdLrange([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "0"},
		{Type: resp.BulkString, Text: "-1"},
	})
	
	if rangeResult.Items[0].Text != "1" {
		t.Errorf("expected sorted list, got %v", rangeResult.Items)
	}
}

func TestCmdSortDesc(t *testing.T) {
	setupListContext()
	
	cmdRpush([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "1"},
		{Type: resp.BulkString, Text: "3"},
		{Type: resp.BulkString, Text: "2"},
	})
	
	args := []resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "DESC"},
	}
	
	cmdSort(args)
	
	rangeResult := cmdLrange([]resp.Value{
		{Type: resp.BulkString, Text: "mylist"},
		{Type: resp.BulkString, Text: "0"},
		{Type: resp.BulkString, Text: "-1"},
	})
	
	if rangeResult.Items[0].Text != "3" {
		t.Errorf("expected descending order, got %v", rangeResult.Items)
	}
}
