package mutation

import "testing"

func TestNewCopiesArguments(t *testing.T) {
	args := []string{"key", "value"}
	command := New("SET", args...)
	args[0] = "changed"
	if command.Name != "SET" || command.Args[0] != "key" || command.Args[1] != "value" {
		t.Fatalf("command = %#v", command)
	}
}
