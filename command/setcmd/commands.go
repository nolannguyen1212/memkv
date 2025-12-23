package setcmd

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

func init() {
	command.Register("SADD", handleAdd)
	command.Register("SREM", handleRemove)
	command.Register("SMEMBERS", handleMembers)
	command.Register("SISMEMBER", handleIsMember)
	command.Register("SCARD", handleCardinality)
	command.Register("SEXPIRE", handleExpire)
	command.Register("STTL", handleTTL)
}

func handleAdd(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("sadd")
	}

	key := arguments[0].String
	members := extractStrings(arguments[1:])
	addedCount := dataStore.Set.Add(key, members...)

	return command.IntegerResponse(int64(addedCount))
}

func handleRemove(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("srem")
	}

	key := arguments[0].String
	members := extractStrings(arguments[1:])
	removedCount := dataStore.Set.Remove(key, members...)

	return command.IntegerResponse(int64(removedCount))
}

func handleMembers(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("smembers")
	}

	members, exists := dataStore.Set.Members(arguments[0].String)
	if !exists {
		return command.NullArrayResponse()
	}

	items := make([]protocol.Value, len(members))
	for i, member := range members {
		items[i] = command.StringResponse(member)
	}

	return command.ArrayResponse(items)
}

func handleIsMember(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("sismember")
	}

	if dataStore.Set.IsMember(arguments[0].String, arguments[1].String) {
		return command.IntegerResponse(1)
	}
	return command.IntegerResponse(0)
}

func handleCardinality(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("scard")
	}

	return command.IntegerResponse(int64(dataStore.Set.Cardinality(arguments[0].String)))
}

func handleExpire(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("sexpire")
	}

	key := arguments[0].String
	seconds, err := strconv.Atoi(arguments[1].String)
	if err != nil {
		return command.NotIntegerError()
	}

	timeToLive := time.Duration(seconds) * time.Second
	success := dataStore.Set.Expire(key, timeToLive)
	if !success {
		return command.IntegerResponse(0)
	}

	return command.IntegerResponse(1)
}

func handleTTL(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("sttl")
	}

	return command.IntegerResponse(dataStore.Set.TTL(arguments[0].String))
}

func extractStrings(values []protocol.Value) []string {
	strings := make([]string, len(values))
	for i, value := range values {
		strings[i] = value.String
	}
	return strings
}
