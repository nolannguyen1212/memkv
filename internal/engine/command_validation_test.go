package engine

import (
	"strings"
	"testing"
)

func TestRegistryRejectsInvalidArgumentCountsBeforeHandler(t *testing.T) {
	commands := []struct {
		name string
		args []string
	}{
		{name: "GET", args: []string{"key", "extra"}},
		{name: "KEYS", args: []string{"*", "extra"}},
		{name: "REPLCONF", args: []string{"listening-port"}},
		{name: "MULTI", args: []string{"extra"}},
		{name: "PING", args: []string{"one", "two"}},
	}

	connection := newTestContext()

	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			result := BootstrapExecute(connection, command.name, command.args)

			if result.Type != ResultError {
				t.Fatalf("result = %#v, want an error", result)
			}

			if !strings.Contains(result.String, "wrong number of arguments") {
				t.Fatalf("error = %q, want argument count error", result.String)
			}
		})
	}
}

func TestTransactionRejectsIncompleteSetOption(t *testing.T) {
	connection := newTestContext()
	BootstrapExecute(connection, "MULTI", nil)

	result := BootstrapExecute(connection, "SET", []string{"key", "value", "PX"})

	if result.Type != ResultError {
		t.Fatalf("result = %#v, want an error", result)
	}

	if !connection.Transaction.Dirty {
		t.Fatal("transaction should be dirty")
	}

	if len(connection.Transaction.Queue) != 0 {
		t.Fatalf("queue = %v, want empty queue", connection.Transaction.Queue)
	}

	execResult := BootstrapExecute(connection, "EXEC", nil)

	if execResult.Type != ResultError {
		t.Fatalf("EXEC result = %#v", execResult)
	}
}
