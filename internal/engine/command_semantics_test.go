package engine

import (
	"reflect"
	"testing"
)

func TestMissingCollectionCommandsReturnEmptyArrays(t *testing.T) {
	connection := newTestContext()
	commands := []struct {
		name string
		args []string
	}{
		{name: "SMEMBERS", args: []string{"missing"}},
		{name: "HGETALL", args: []string{"missing"}},
		{name: "LRANGE", args: []string{"missing", "0", "-1"}},
		{name: "ZRANGE", args: []string{"missing", "0", "-1"}},
	}

	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			result := BootstrapExecute(connection, command.name, command.args)

			if result.Type != ResultArray {
				t.Fatalf("type = %v, want array", result.Type)
			}

			if result.IsNull {
				t.Fatalf("result = %#v, want non-null array", result)
			}

			if len(result.Array) != 0 {
				t.Fatalf("array = %#v, want empty array", result.Array)
			}
		})
	}
}

func TestListPopReplyDependsOnCountArgument(t *testing.T) {
	connection := newTestContext()
	result := BootstrapExecute(connection, "LPOP", []string{"missing"})

	if result.Type != ResultBulkString || !result.IsNull {
		t.Fatalf("LPOP missing = %#v, want null bulk string", result)
	}

	result = BootstrapExecute(connection, "LPOP", []string{"missing", "2"})

	if result.Type != ResultArray {
		t.Fatalf("LPOP type = %v, want array", result.Type)
	}

	if result.IsNull {
		t.Fatalf("LPOP result = %#v, want non-null array", result)
	}

	if len(result.Array) != 0 {
		t.Fatalf("LPOP array = %#v, want empty array", result.Array)
	}

	BootstrapExecute(connection, "RPUSH", []string{"list", "a", "b", "c"})
	result = BootstrapExecute(connection, "LPOP", []string{"list"})

	if result.Type != ResultBulkString || result.String != "a" {
		t.Fatalf("LPOP = %#v, want a", result)
	}

	result = BootstrapExecute(connection, "RPOP", []string{"list", "2"})
	values := resultStrings(result.Array)

	if result.Type != ResultArray || !reflect.DeepEqual(values, []string{"c", "b"}) {
		t.Fatalf("RPOP count = %#v", result)
	}
}

func TestUnorderedCollectionRepliesAreDeterministic(t *testing.T) {
	connection := newTestContext()
	BootstrapExecute(connection, "SADD", []string{"set", "c", "a", "b"})
	setMembers := BootstrapExecute(connection, "SMEMBERS", []string{"set"})

	if values := resultStrings(setMembers.Array); !reflect.DeepEqual(values, []string{"a", "b", "c"}) {
		t.Fatalf("SMEMBERS = %v", values)
	}

	BootstrapExecute(connection, "HSET", []string{"hash", "c", "3", "a", "1", "b", "2"})
	hashFields := BootstrapExecute(connection, "HGETALL", []string{"hash"})

	if values := resultStrings(hashFields.Array); !reflect.DeepEqual(values, []string{"a", "1", "b", "2", "c", "3"}) {
		t.Fatalf("HGETALL = %v", values)
	}

	BootstrapExecute(connection, "SET", []string{"key:c", "3"})
	BootstrapExecute(connection, "SET", []string{"key:a", "1"})
	BootstrapExecute(connection, "SET", []string{"key:b", "2"})
	keys := BootstrapExecute(connection, "KEYS", []string{"key:*"})

	if values := resultStrings(keys.Array); !reflect.DeepEqual(values, []string{"key:a", "key:b", "key:c"}) {
		t.Fatalf("KEYS = %v", values)
	}
}

func resultStrings(results []Result) []string {
	values := make([]string, len(results))

	for index, result := range results {
		values[index] = result.String
	}

	return values
}
