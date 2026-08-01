package engine

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerStringCommands(registry *Registry) {
	registry.Register("SET", handleSet)
	registry.Register("GET", handleGet)
	registry.Register("DEL", handleDel)
	registry.Register("EXPIRE", handleExpire)
	registry.Register("PEXPIREAT", handlePExpireAt)
	registry.Register("TTL", handleTTL)
	registry.Register("PING", handlePing)
	registry.Register("FLUSHDB", handleFlush)
	registry.Register("FLUSHALL", handleFlush)
}

func handleSet(connContext *ConnContext, args []resp.Value) resp.Value {
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

	connContext.Store.PrepareWrite(key, store.KeyTypeString, true)
	connContext.Store.Dictionary.Set(key, value, 0)
	connContext.OnKeyWrite(key)
	if err := connContext.AppendWAL(buildBulkArray("SET", key, value)); err != nil {
		return persistenceError()
	}

	if ttl > 0 {
		expAt := time.Now().Add(ttl)
		connContext.Store.Keyspace.ExpireAt(key, expAt)
		if err := connContext.AppendWAL(buildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10))); err != nil {
			return persistenceError()
		}
	}

	return okReply()
}

func handleGet(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgCountError("get")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeString); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	value, exists := connContext.Store.Dictionary.Get(key)
	if !exists {
		return nullStringReply()
	}
	return stringReply(value)
}

func handleDel(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgCountError("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.String
	}

	count := 0
	for _, key := range keys {
		if connContext.Store.DeleteKey(key) {
			count++
			connContext.OnKeyMutate(key)
		}
	}
	if err := connContext.AppendWAL(buildBulkArray(append([]string{"DEL"}, keys...)...)); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleExpire(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("expire")
	}

	key := args[0].String
	seconds, err := strconv.Atoi(args[1].String)
	if err != nil {
		return notIntegerError()
	}

	ttl := time.Duration(seconds) * time.Second
	if !connContext.Store.Keyspace.Expire(key, ttl) {
		return intReply(0)
	}
	connContext.OnKeyMutate(key)

	expAt := time.Now().Add(ttl)
	if err := connContext.AppendWAL(buildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10))); err != nil {
		return persistenceError()
	}

	return intReply(1)
}

func handlePExpireAt(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("pexpireat")
	}

	key := args[0].String
	ms, err := strconv.ParseInt(args[1].String, 10, 64)
	if err != nil {
		return errorReply("ERR invalid expire time")
	}

	if !connContext.Store.Keyspace.ExpireAt(key, time.UnixMilli(ms)) {
		return intReply(0)
	}
	connContext.OnKeyMutate(key)
	if err := connContext.AppendWAL(buildBulkArray("PEXPIREAT", key, args[1].String)); err != nil {
		return persistenceError()
	}
	return intReply(1)
}

func handleTTL(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("ttl")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(connContext.Store.Keyspace.TTL(args[0].String))
}

func handlePing(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return pongReply()
	}
	return stringReply(args[0].String)
}

func handleFlush(connContext *ConnContext, _ []resp.Value) resp.Value {
	for _, key := range connContext.Store.Clear() {
		globalWatch.Notify(key)
	}
	if err := connContext.AppendWAL(buildBulkArray("FLUSHALL")); err != nil {
		return persistenceError()
	}
	return okReply()
}
