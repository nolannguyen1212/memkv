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
		return wrongArgCountError("set")
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
	connContext.AppendAOF(buildBulkArray("SET", key, value))

	if ttl > 0 {
		expAt := time.Now().Add(ttl)
		connContext.AppendAOF(buildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))
	}

	return okReply()
}

func handleGet(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgCountError("get")
	}

	connContext.OnKeyRead(args[0].String)
	value, exists := connContext.Store.Dictionary.Get(args[0].String)
	if !exists {
		return nullStringReply()
	}
	return stringReply(value)
}

func handleDel(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgCountError("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.String
	}

	count := connContext.Store.Dictionary.Delete(keys...)
	for _, key := range keys {
		connContext.OnKeyDelete(key)
	}
	connContext.AppendAOF(buildBulkArray(append([]string{"DEL"}, keys...)...))

	return intReply(int64(count))
}

func handleExpire(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("expire")
	}

	key := args[0].String
	seconds, err := strconv.Atoi(args[1].String)
	if err != nil {
		return notIntegerError()
	}

	ttl := time.Duration(seconds) * time.Second
	if !connContext.Store.Dictionary.Expire(key, ttl) {
		return intReply(0)
	}

	expAt := time.Now().Add(ttl)
	connContext.AppendAOF(buildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))

	return intReply(1)
}

func handlePExpireAt(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("pexpireat")
	}

	key := args[0].String
	ms, err := strconv.ParseInt(args[1].String, 10, 64)
	if err != nil {
		return errorReply("ERR invalid expire time")
	}

	if !connContext.Store.Dictionary.ExpireAt(key, time.UnixMilli(ms)) {
		return intReply(0)
	}
	return intReply(1)
}

func handleTTL(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("ttl")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(connContext.Store.Dictionary.TTL(args[0].String))
}

func handlePing(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return pongReply()
	}
	return stringReply(args[0].String)
}
