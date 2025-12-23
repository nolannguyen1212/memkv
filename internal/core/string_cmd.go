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

func handleSet(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("set")
	}

	key := args[0].String
	value := args[1].String
	var ttl time.Duration

	if len(args) > 2 {
		if s, err := strconv.Atoi(args[2].String); err == nil && s > 0 {
			ttl = time.Duration(s) * time.Second
		}
	}

	ctx.Store.Dictionary.Set(key, value, ttl)
	ctx.OnKeyWrite(key)
	ctx.AppendAOF(BuildBulkArray("SET", key, value))

	if ttl > 0 {
		expAt := time.Now().Add(ttl)
		ctx.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))
	}

	return OKResponse()
}

func handleGet(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("get")
	}

	ctx.OnKeyRead(args[0].String)
	value, exists := ctx.Store.Dictionary.Get(args[0].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(value)
}

func handleDel(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("del")
	}

	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.String
	}

	count := ctx.Store.Dictionary.Delete(keys...)
	for _, k := range keys {
		ctx.OnKeyDelete(k)
	}
	ctx.AppendAOF(BuildBulkArray(append([]string{"DEL"}, keys...)...))

	return IntegerResponse(int64(count))
}

func handleExpire(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("expire")
	}

	key := args[0].String
	secs, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NotIntegerError()
	}

	ttl := time.Duration(secs) * time.Second
	if !ctx.Store.Dictionary.Expire(key, ttl) {
		return IntegerResponse(0)
	}

	expAt := time.Now().Add(ttl)
	ctx.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))

	return IntegerResponse(1)
}

func handlePExpireAt(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("pexpireat")
	}

	key := args[0].String
	ms, err := strconv.ParseInt(args[1].String, 10, 64)
	if err != nil {
		return ErrorResponse("ERR invalid expire time")
	}

	if !ctx.Store.Dictionary.ExpireAt(key, time.UnixMilli(ms)) {
		return IntegerResponse(0)
	}
	return IntegerResponse(1)
}

func handleTTL(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("ttl")
	}

	ctx.OnKeyRead(args[0].String)
	return IntegerResponse(ctx.Store.Dictionary.TTL(args[0].String))
}

func handlePing(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return PongResponse()
	}
	return StringResponse(args[0].String)
}
