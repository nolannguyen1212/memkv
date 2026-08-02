package mutation

import "testing"

func TestNewCopiesArguments(t *testing.T) {
	args := []string{"key", "value"}
	command := New("SET", args...)
	args[0] = "changed"

	if command.Name != "SET" {
		t.Fatalf("name = %q, want SET", command.Name)
	}

	if command.Args[0] != "key" {
		t.Fatalf("key = %q, want key", command.Args[0])
	}

	if command.Args[1] != "value" {
		t.Fatalf("value = %q, want value", command.Args[1])
	}
}
