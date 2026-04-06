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
		return wrongArgCountError("hset")
	}

	key := args[0].String
	fieldValues := extractStrings(args[1:])
	count := connContext.Store.Hash.Set(key, fieldValues...)

	connContext.OnKeyWrite(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"HSET", key}, fieldValues...)...))

	return intReply(int64(count))
}

func handleHget(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("hget")
	}

	connContext.OnKeyRead(args[0].String)
	value, exists := connContext.Store.Hash.Get(args[0].String, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return stringReply(value)
}

func handleHdel(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("hdel")
	}

	key := args[0].String
	fields := extractStrings(args[1:])
	count := connContext.Store.Hash.Delete(key, fields...)

	connContext.OnKeyMutate(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"HDEL", key}, fields...)...))

	return intReply(int64(count))
}

func handleHgetall(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("hgetall")
	}

	connContext.OnKeyRead(args[0].String)
	fields, exists := connContext.Store.Hash.GetAll(args[0].String)
	if !exists {
		return emptyArrayReply()
	}

	items := make([]protocol.Value, 0, len(fields)*2)
	for field, value := range fields {
		items = append(items, stringReply(field))
		items = append(items, stringReply(value))
	}
	return arrayReply(items)
}

func handleHexists(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("hexists")
	}

	connContext.OnKeyRead(args[0].String)
	if connContext.Store.Hash.Exists(args[0].String, args[1].String) {
		return intReply(1)
	}
	return intReply(0)
}

func handleHlen(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("hlen")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(int64(connContext.Store.Hash.FieldCount(args[0].String)))
}
