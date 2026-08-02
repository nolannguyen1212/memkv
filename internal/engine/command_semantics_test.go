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
			result := Execute(connection, command.name, command.args)
			if result.Type != ResultArray || result.IsNull || len(result.Array) != 0 {
				t.Fatalf("result = %#v, want empty array", result)
			}
		})
	}
}

func TestListPopReplyDependsOnCountArgument(t *testing.T) {
	connection := newTestContext()
	if result := Execute(connection, "LPOP", []string{"missing"}); result.Type != ResultBulkString || !result.IsNull {
		t.Fatalf("LPOP missing = %#v, want null bulk string", result)
	}
	if result := Execute(connection, "LPOP", []string{"missing", "2"}); result.Type != ResultArray || result.IsNull || len(result.Array) != 0 {
		t.Fatalf("LPOP missing count = %#v, want empty array", result)
	}
	Execute(connection, "RPUSH", []string{"list", "a", "b", "c"})
	if result := Execute(connection, "LPOP", []string{"list"}); result.Type != ResultBulkString || result.String != "a" {
		t.Fatalf("LPOP = %#v, want a", result)
	}
	result := Execute(connection, "RPOP", []string{"list", "2"})
	if result.Type != ResultArray || len(result.Array) != 2 || result.Array[0].String != "c" || result.Array[1].String != "b" {
		t.Fatalf("RPOP count = %#v", result)
	}
}

func TestUnorderedCollectionRepliesAreDeterministic(t *testing.T) {
	connection := newTestContext()
	Execute(connection, "SADD", []string{"set", "c", "a", "b"})
	setMembers := Execute(connection, "SMEMBERS", []string{"set"})
	if values := resultStrings(setMembers.Array); !reflect.DeepEqual(values, []string{"a", "b", "c"}) {
		t.Fatalf("SMEMBERS = %v", values)
	}
	Execute(connection, "HSET", []string{"hash", "c", "3", "a", "1", "b", "2"})
	hashFields := Execute(connection, "HGETALL", []string{"hash"})
	if values := resultStrings(hashFields.Array); !reflect.DeepEqual(values, []string{"a", "1", "b", "2", "c", "3"}) {
		t.Fatalf("HGETALL = %v", values)
	}
	Execute(connection, "SET", []string{"key:c", "3"})
	Execute(connection, "SET", []string{"key:a", "1"})
	Execute(connection, "SET", []string{"key:b", "2"})
	keys := Execute(connection, "KEYS", []string{"key:*"})
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
