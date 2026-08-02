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
			result := Execute(connection, command.name, command.args)
			if result.Type != ResultError || !strings.Contains(result.String, "wrong number of arguments") {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestTransactionRejectsIncompleteSetOption(t *testing.T) {
	connection := newTestContext()
	Execute(connection, "MULTI", nil)
	result := Execute(connection, "SET", []string{"key", "value", "PX"})
	if result.Type != ResultError || !connection.Transaction.Dirty || len(connection.Transaction.Queue) != 0 {
		t.Fatalf("result = %#v, transaction = %#v", result, connection.Transaction)
	}
	execResult := Execute(connection, "EXEC", nil)
	if execResult.Type != ResultError {
		t.Fatalf("EXEC result = %#v", execResult)
	}
}
