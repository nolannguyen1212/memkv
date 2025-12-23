package stringcmd

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

func init() {
	command.Register("SET", handleSet)
	command.Register("GET", handleGet)
	command.Register("DEL", handleDelete)
	command.Register("EXPIRE", handleExpire)
	command.Register("PEXPIREAT", handlePExpireAt)
	command.Register("TTL", handleTTL)
	command.Register("PING", handlePing)
}

func handleSet(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("set")
	}

	key := arguments[0].String
	value := arguments[1].String
	var timeToLive time.Duration

	if len(arguments) > 2 {
		if seconds, err := strconv.Atoi(arguments[2].String); err == nil && seconds > 0 {
			timeToLive = time.Duration(seconds) * time.Second
		}
	}

	dataStore.Dictionary.Set(key, value, timeToLive)
	return command.OKResponse()
}

func handleGet(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 {
		return command.WrongArgumentCountError("get")
	}

	value, exists := dataStore.Dictionary.Get(arguments[0].String)
	if !exists {
		return command.NullStringResponse()
	}

	return command.StringResponse(value)
}

func handleDelete(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 {
		return command.WrongArgumentCountError("del")
	}

	keys := make([]string, len(arguments))
	for i, arg := range arguments {
		keys[i] = arg.String
	}

	deletedCount := dataStore.Dictionary.Delete(keys...)
	return command.IntegerResponse(int64(deletedCount))
}

func handleExpire(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("expire")
	}

	key := arguments[0].String
	seconds, err := strconv.Atoi(arguments[1].String)
	if err != nil {
		return command.NotIntegerError()
	}

	timeToLive := time.Duration(seconds) * time.Second
	success := dataStore.Dictionary.Expire(key, timeToLive)
	if !success {
		return command.IntegerResponse(0)
	}

	return command.IntegerResponse(1)
}

func handlePExpireAt(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("pexpireat")
	}

	key := arguments[0].String
	milliseconds, err := strconv.ParseInt(arguments[1].String, 10, 64)
	if err != nil {
		return command.ErrorResponse("ERR invalid expire time")
	}

	expireAt := time.UnixMilli(milliseconds)
	success := dataStore.Dictionary.ExpireAt(key, expireAt)
	if !success {
		return command.IntegerResponse(0)
	}

	return command.IntegerResponse(1)
}

func handleTTL(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("ttl")
	}

	remaining := dataStore.Dictionary.TTL(arguments[0].String)
	return command.IntegerResponse(remaining)
}

func handlePing(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) == 0 {
		return command.PongResponse()
	}
	return command.StringResponse(arguments[0].String)
}
