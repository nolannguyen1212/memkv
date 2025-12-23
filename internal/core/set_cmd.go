package core

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	Register("SADD", handleSadd)
	Register("SREM", handleSrem)
	Register("SMEMBERS", handleSmembers)
	Register("SISMEMBER", handleSismember)
	Register("SCARD", handleScard)
	Register("SEXPIRE", handleSexpire)
	Register("STTL", handleSttl)
}

func handleSadd(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("sadd")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := ctx.Store.Set.Add(key, members...)

	ctx.OnKeyWrite(key)
	ctx.AppendAOF(BuildBulkArray(append([]string{"SADD", key}, members...)...))

	return IntegerResponse(int64(count))
}

func handleSrem(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("srem")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := ctx.Store.Set.Remove(key, members...)

	ctx.AppendAOF(BuildBulkArray(append([]string{"SREM", key}, members...)...))

	return IntegerResponse(int64(count))
}

func handleSmembers(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("smembers")
	}

	ctx.OnKeyRead(args[0].String)
	members, exists := ctx.Store.Set.Members(args[0].String)
	if !exists {
		return NullArrayResponse()
	}

	items := make([]protocol.Value, len(members))
	for i, m := range members {
		items[i] = StringResponse(m)
	}
	return ArrayResponse(items)
}

func handleSismember(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("sismember")
	}

	ctx.OnKeyRead(args[0].String)
	if ctx.Store.Set.IsMember(args[0].String, args[1].String) {
		return IntegerResponse(1)
	}
	return IntegerResponse(0)
}

func handleScard(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("scard")
	}

	ctx.OnKeyRead(args[0].String)
	return IntegerResponse(int64(ctx.Store.Set.Cardinality(args[0].String)))
}

func handleSexpire(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("sexpire")
	}

	key := args[0].String
	secs, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NotIntegerError()
	}

	if !ctx.Store.Set.Expire(key, time.Duration(secs)*time.Second) {
		return IntegerResponse(0)
	}
	return IntegerResponse(1)
}

func handleSttl(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("sttl")
	}

	ctx.OnKeyRead(args[0].String)
	return IntegerResponse(ctx.Store.Set.TTL(args[0].String))
}

func extractStrings(args []protocol.Value) []string {
	result := make([]string, len(args))
	for i, a := range args {
		result[i] = a.String
	}
	return result
}
