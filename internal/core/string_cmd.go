package core

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	Register("SET", handleSet)
	Register("GET", handleGet)
	Register("DEL", handleDel)
	Register("EXPIRE", handleExpire)
	Register("PEXPIREAT", handlePExpireAt)
	Register("TTL", handleTTL)
	Register("PING", handlePing)
}

func handleSet(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("set")
	}

	key := args[0].String
	value := args[1].String
	var ttl time.Duration

	if len(args) > 2 {
		if seconds, err := strconv.Atoi(args[2].String); err == nil && seconds > 0 {
			ttl = time.Duration(seconds) * time.Second
		}
	}

	connContext.Store.Dictionary.Set(key, value, ttl)
	connContext.OnKeyWrite(key)
	connContext.AppendAOF(BuildBulkArray("SET", key, value))

	if ttl > 0 {
		expAt := time.Now().Add(ttl)
		connContext.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))
	}

	return OKResponse()
}

func handleGet(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("get")
	}

	connContext.OnKeyRead(args[0].String)
	value, exists := connContext.Store.Dictionary.Get(args[0].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(value)
}

func handleDel(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.String
	}

	count := connContext.Store.Dictionary.Delete(keys...)
	for _, key := range keys {
		connContext.OnKeyDelete(key)
	}
	connContext.AppendAOF(BuildBulkArray(append([]string{"DEL"}, keys...)...))

	return IntegerResponse(int64(count))
}

func handleExpire(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("expire")
	}

	key := args[0].String
	seconds, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NotIntegerError()
	}

	ttl := time.Duration(seconds) * time.Second
	if !connContext.Store.Dictionary.Expire(key, ttl) {
		return IntegerResponse(0)
	}

	expAt := time.Now().Add(ttl)
	connContext.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))

	return IntegerResponse(1)
}

func handlePExpireAt(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("pexpireat")
	}

	key := args[0].String
	ms, err := strconv.ParseInt(args[1].String, 10, 64)
	if err != nil {
		return ErrorResponse("ERR invalid expire time")
	}

	if !connContext.Store.Dictionary.ExpireAt(key, time.UnixMilli(ms)) {
		return IntegerResponse(0)
	}
	return IntegerResponse(1)
}

func handleTTL(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("ttl")
	}

	connContext.OnKeyRead(args[0].String)
	return IntegerResponse(connContext.Store.Dictionary.TTL(args[0].String))
}

func handlePing(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return PongResponse()
	}
	return StringResponse(args[0].String)
}
