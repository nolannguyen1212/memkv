package storage

import "testing"

func TestListPushPop(t *testing.T) {
	list := NewList()

	list.LeftPush("mylist", "a", "b")
	list.RightPush("mylist", "c", "d")

	if list.Length("mylist") != 4 {
		t.Errorf("Expected length 4, got %d", list.Length("mylist"))
	}

	popped := list.LeftPop("mylist", 1)
	if len(popped) != 1 || popped[0] != "b" {
		t.Errorf("Expected 'b', got %v", popped)
	}

	popped = list.RightPop("mylist", 1)
	if len(popped) != 1 || popped[0] != "d" {
		t.Errorf("Expected 'd', got %v", popped)
	}
}

func TestListRange(t *testing.T) {
	list := NewList()
	list.RightPush("mylist", "a", "b", "c", "d", "e")

	values, exists := list.Range("mylist", 1, 3)
	if !exists || len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "b" || values[1] != "c" || values[2] != "d" {
		t.Errorf("Expected [b,c,d], got %v", values)
	}
}

func TestListSort(t *testing.T) {
	list := NewList()
	list.RightPush("mylist", "3", "1", "2")

	list.Sort("mylist", true, false)

	values, _ := list.Range("mylist", 0, -1)
	if values[0] != "1" || values[1] != "2" || values[2] != "3" {
		t.Errorf("Expected sorted [1,2,3], got %v", values)
	}
}
