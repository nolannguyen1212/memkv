package hashcmd

import (
	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

func init() {
	command.Register("HSET", handleSet)
	command.Register("HGET", handleGet)
	command.Register("HDEL", handleDelete)
	command.Register("HGETALL", handleGetAll)
	command.Register("HEXISTS", handleExists)
	command.Register("HLEN", handleLength)
}

func handleSet(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 3 || len(arguments)%2 == 0 {
		return command.WrongArgumentCountError("hset")
	}

	key := arguments[0].String
	fieldValues := extractStrings(arguments[1:])
	addedCount := dataStore.HashMap.Set(key, fieldValues...)

	return command.IntegerResponse(int64(addedCount))
}

func handleGet(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("hget")
	}

	value, exists := dataStore.HashMap.Get(arguments[0].String, arguments[1].String)
	if !exists {
		return command.NullStringResponse()
	}

	return command.StringResponse(value)
}

func handleDelete(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("hdel")
	}

	key := arguments[0].String
	fields := extractStrings(arguments[1:])
	deletedCount := dataStore.HashMap.Delete(key, fields...)

	return command.IntegerResponse(int64(deletedCount))
}

func handleGetAll(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("hgetall")
	}

	hash, exists := dataStore.HashMap.GetAll(arguments[0].String)
	if !exists {
		return command.EmptyArrayResponse()
	}

	items := make([]protocol.Value, 0, len(hash)*2)
	for field, value := range hash {
		items = append(items, command.StringResponse(field))
		items = append(items, command.StringResponse(value))
	}

	return command.ArrayResponse(items)
}

func handleExists(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("hexists")
	}

	if dataStore.HashMap.Exists(arguments[0].String, arguments[1].String) {
		return command.IntegerResponse(1)
	}
	return command.IntegerResponse(0)
}

func handleLength(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("hlen")
	}

	return command.IntegerResponse(int64(dataStore.HashMap.FieldCount(arguments[0].String)))
}

func extractStrings(values []protocol.Value) []string {
	strings := make([]string, len(values))
	for i, value := range values {
		strings[i] = value.String
	}
	return strings
}
