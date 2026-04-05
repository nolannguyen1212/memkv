package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

func init() {
	Register("HSET", handleHset)
	Register("HGET", handleHget)
	Register("HDEL", handleHdel)
	Register("HGETALL", handleHgetall)
	Register("HEXISTS", handleHexists)
	Register("HLEN", handleHlen)
}

func handleHset(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return WrongArgCountError("hset")
	}

	key := args[0].String
	fieldValues := extractStrings(args[1:])
	count := cctx.Store.HashMap.Set(key, fieldValues...)

	cctx.OnKeyWrite(key)
	cctx.AppendAOF(BuildBulkArray(append([]string{"HSET", key}, fieldValues...)...))

	return IntegerResponse(int64(count))
}

func handleHget(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("hget")
	}

	cctx.OnKeyRead(args[0].String)
	value, exists := cctx.Store.HashMap.Get(args[0].String, args[1].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(value)
}

func handleHdel(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("hdel")
	}

	key := args[0].String
	fields := extractStrings(args[1:])
	count := cctx.Store.HashMap.Delete(key, fields...)

	cctx.OnKeyMutate(key)
	cctx.AppendAOF(BuildBulkArray(append([]string{"HDEL", key}, fields...)...))

	return IntegerResponse(int64(count))
}

func handleHgetall(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("hgetall")
	}

	cctx.OnKeyRead(args[0].String)
	hash, exists := cctx.Store.HashMap.GetAll(args[0].String)
	if !exists {
		return EmptyArrayResponse()
	}

	items := make([]protocol.Value, 0, len(hash)*2)
	for field, value := range hash {
		items = append(items, StringResponse(field))
		items = append(items, StringResponse(value))
	}
	return ArrayResponse(items)
}

func handleHexists(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("hexists")
	}

	cctx.OnKeyRead(args[0].String)
	if cctx.Store.HashMap.Exists(args[0].String, args[1].String) {
		return IntegerResponse(1)
	}
	return IntegerResponse(0)
}

func handleHlen(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("hlen")
	}

	cctx.OnKeyRead(args[0].String)
	return IntegerResponse(int64(cctx.Store.HashMap.FieldCount(args[0].String)))
}
