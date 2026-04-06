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

func handleHset(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return WrongArgCountError("hset")
	}

	key := args[0].String
	fieldValues := extractStrings(args[1:])
	count := connContext.Store.HashMap.Set(key, fieldValues...)

	connContext.OnKeyWrite(key)
	connContext.AppendAOF(BuildBulkArray(append([]string{"HSET", key}, fieldValues...)...))

	return IntegerResponse(int64(count))
}

func handleHget(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("hget")
	}

	connContext.OnKeyRead(args[0].String)
	value, exists := connContext.Store.HashMap.Get(args[0].String, args[1].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(value)
}

func handleHdel(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("hdel")
	}

	key := args[0].String
	fields := extractStrings(args[1:])
	count := connContext.Store.HashMap.Delete(key, fields...)

	connContext.OnKeyMutate(key)
	connContext.AppendAOF(BuildBulkArray(append([]string{"HDEL", key}, fields...)...))

	return IntegerResponse(int64(count))
}

func handleHgetall(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("hgetall")
	}

	connContext.OnKeyRead(args[0].String)
	hash, exists := connContext.Store.HashMap.GetAll(args[0].String)
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

func handleHexists(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("hexists")
	}

	connContext.OnKeyRead(args[0].String)
	if connContext.Store.HashMap.Exists(args[0].String, args[1].String) {
		return IntegerResponse(1)
	}
	return IntegerResponse(0)
}

func handleHlen(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("hlen")
	}

	connContext.OnKeyRead(args[0].String)
	return IntegerResponse(int64(connContext.Store.HashMap.FieldCount(args[0].String)))
}
