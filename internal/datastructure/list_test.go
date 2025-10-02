package datastructure

import (
	"testing"
)

func TestListLpush(t *testing.T) {
	list := CreateList()
	
	n := list.Lpush("mylist", "a", "b", "c")
	if n != 3 {
		t.Errorf("expected 3, got %d", n)
	}
	
	if list.Llen("mylist") != 3 {
		t.Errorf("expected length 3, got %d", list.Llen("mylist"))
	}
}

func TestListRpush(t *testing.T) {
	list := CreateList()
	
	n := list.Rpush("mylist", "a", "b", "c")
	if n != 3 {
		t.Errorf("expected 3, got %d", n)
	}
	
	if list.Llen("mylist") != 3 {
		t.Errorf("expected length 3, got %d", list.Llen("mylist"))
	}
}

func TestListLpop(t *testing.T) {
	list := CreateList()
	list.Lpush("mylist", "c", "b", "a")
	
	items := list.Lpop("mylist", 2)
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	if items[0].Value != "a" {
		t.Errorf("expected 'a', got %s", items[0].Value)
	}
	if items[1].Value != "b" {
		t.Errorf("expected 'b', got %s", items[1].Value)
	}
	
	if list.Llen("mylist") != 1 {
		t.Errorf("expected length 1, got %d", list.Llen("mylist"))
	}
}

func TestListRpop(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "a", "b", "c")
	
	items := list.Rpop("mylist", 2)
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	if items[0].Value != "c" {
		t.Errorf("expected 'c', got %s", items[0].Value)
	}
	if items[1].Value != "b" {
		t.Errorf("expected 'b', got %s", items[1].Value)
	}
	
	if list.Llen("mylist") != 1 {
		t.Errorf("expected length 1, got %d", list.Llen("mylist"))
	}
}

func TestListLrange(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "a", "b", "c", "d", "e")
	
	items, ok := list.Lrange("mylist", 1, 3)
	if !ok {
		t.Error("expected ok to be true")
	}
	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d", len(items))
	}
	if items[0].Value != "b" || items[1].Value != "c" || items[2].Value != "d" {
		t.Errorf("unexpected values: %v", items)
	}
}

func TestListLrangeNegativeIndices(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "a", "b", "c", "d", "e")
	
	items, ok := list.Lrange("mylist", -3, -1)
	if !ok {
		t.Error("expected ok to be true")
	}
	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d", len(items))
	}
	if items[0].Value != "c" || items[1].Value != "d" || items[2].Value != "e" {
		t.Errorf("unexpected values: %v", items)
	}
}

func TestListLrangeEmpty(t *testing.T) {
	list := CreateList()
	
	_, ok := list.Lrange("nonexistent", 0, 10)
	if ok {
		t.Error("expected ok to be false for nonexistent key")
	}
}

func TestListSort(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "3", "1", "2")
	
	list.Sort("mylist", true, false)
	
	items, _ := list.Lrange("mylist", 0, -1)
	if items[0].Value != "1" || items[1].Value != "2" || items[2].Value != "3" {
		t.Errorf("expected sorted [1,2,3], got %v", items)
	}
}

func TestListSortDescending(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "1", "3", "2")
	
	list.Sort("mylist", false, false)
	
	items, _ := list.Lrange("mylist", 0, -1)
	if items[0].Value != "3" || items[1].Value != "2" || items[2].Value != "1" {
		t.Errorf("expected sorted [3,2,1], got %v", items)
	}
}

func TestListSortAlpha(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "banana", "apple", "cherry")
	
	list.Sort("mylist", true, true)
	
	items, _ := list.Lrange("mylist", 0, -1)
	if items[0].Value != "apple" || items[1].Value != "banana" || items[2].Value != "cherry" {
		t.Errorf("expected sorted [apple,banana,cherry], got %v", items)
	}
}

func TestListPopUntilEmpty(t *testing.T) {
	list := CreateList()
	list.Rpush("mylist", "a", "b")
	
	list.Lpop("mylist", 2)
	
	if list.Llen("mylist") != 0 {
		t.Errorf("expected length 0, got %d", list.Llen("mylist"))
	}
	
	items := list.Lpop("mylist", 1)
	if len(items) != 0 {
		t.Errorf("expected empty array, got %d items", len(items))
	}
}

func TestListDump(t *testing.T) {
	list := CreateList()
	list.Rpush("list1", "a", "b", "c")
	list.Rpush("list2", "x", "y")
	
	snapshot := list.Dump()
	
	if len(snapshot) != 2 {
		t.Errorf("expected 2 lists in snapshot, got %d", len(snapshot))
	}
	
	if len(snapshot["list1"]) != 3 {
		t.Errorf("expected list1 to have 3 items, got %d", len(snapshot["list1"]))
	}
	
	if len(snapshot["list2"]) != 2 {
		t.Errorf("expected list2 to have 2 items, got %d", len(snapshot["list2"]))
	}
}

func TestListConcurrency(t *testing.T) {
	list := CreateList()
	done := make(chan bool)
	
	for i := 0; i < 10; i++ {
		go func(n int) {
			list.Rpush("concurrent", "item")
			list.Lpop("concurrent", 1)
			done <- true
		}(i)
	}
	
	for i := 0; i < 10; i++ {
		<-done
	}
}
