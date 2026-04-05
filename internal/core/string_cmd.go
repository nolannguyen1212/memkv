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

func handleSet(cctx *ConnContext, args []protocol.Value) protocol.Value {
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

	cctx.Store.Dictionary.Set(key, value, ttl)
	cctx.OnKeyWrite(key)
	cctx.AppendAOF(BuildBulkArray("SET", key, value))

	if ttl > 0 {
		expAt := time.Now().Add(ttl)
		cctx.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))
	}

	return OKResponse()
}

func handleGet(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("get")
	}

	cctx.OnKeyRead(args[0].String)
	value, exists := cctx.Store.Dictionary.Get(args[0].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(value)
}

func handleDel(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("del")
	}

	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.String
	}

	count := cctx.Store.Dictionary.Delete(keys...)
	for _, k := range keys {
		cctx.OnKeyDelete(k)
	}
	cctx.AppendAOF(BuildBulkArray(append([]string{"DEL"}, keys...)...))

	return IntegerResponse(int64(count))
}

func handleExpire(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("expire")
	}

	key := args[0].String
	secs, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NotIntegerError()
	}

	ttl := time.Duration(secs) * time.Second
	if !cctx.Store.Dictionary.Expire(key, ttl) {
		return IntegerResponse(0)
	}

	expAt := time.Now().Add(ttl)
	cctx.AppendAOF(BuildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10)))

	return IntegerResponse(1)
}

func handlePExpireAt(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("pexpireat")
	}

	key := args[0].String
	ms, err := strconv.ParseInt(args[1].String, 10, 64)
	if err != nil {
		return ErrorResponse("ERR invalid expire time")
	}

	if !cctx.Store.Dictionary.ExpireAt(key, time.UnixMilli(ms)) {
		return IntegerResponse(0)
	}
	return IntegerResponse(1)
}

func handleTTL(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("ttl")
	}

	cctx.OnKeyRead(args[0].String)
	return IntegerResponse(cctx.Store.Dictionary.TTL(args[0].String))
}

func handlePing(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return PongResponse()
	}
	return StringResponse(args[0].String)
}
