package engine

import (
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerHashCommands(registry *Registry) {
	registry.Register("HSET", handleHset)
	registry.Register("HGET", handleHget)
	registry.Register("HDEL", handleHdel)
	registry.Register("HGETALL", handleHgetall)
	registry.Register("HEXISTS", handleHexists)
	registry.Register("HLEN", handleHlen)
}

func handleHset(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgCountError("hset")
	}

	key := args[0].String
	fieldValues := extractStrings(args[1:])
	if !connContext.Store.PrepareWrite(key, store.KeyTypeHash, false) {
		return wrongTypeError()
	}
	count := connContext.Store.Hash.Set(key, fieldValues...)

	connContext.OnKeyWrite(key)
	if err := connContext.AppendWAL(buildBulkArray(append([]string{"HSET", key}, fieldValues...)...)); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleHget(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("hget")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeHash); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	value, exists := connContext.Store.Hash.Get(key, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return stringReply(value)
}

func handleHdel(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("hdel")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeHash); !valid {
		return wrongTypeError()
	}
	fields := extractStrings(args[1:])
	count := connContext.Store.Hash.Delete(key, fields...)

	if count > 0 {
		connContext.OnKeyMutate(key)
		if connContext.Store.Hash.FieldCount(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, store.KeyTypeHash)
		}
		if err := connContext.AppendWAL(buildBulkArray(append([]string{"HDEL", key}, fields...)...)); err != nil {
			return persistenceError()
		}
	}

	return intReply(int64(count))
}

func handleHgetall(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("hgetall")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeHash); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	fields, exists := connContext.Store.Hash.GetAll(key)
	if !exists {
		return emptyArrayReply()
	}

	items := make([]resp.Value, 0, len(fields)*2)
	for field, value := range fields {
		items = append(items, stringReply(field))
		items = append(items, stringReply(value))
	}
	return arrayReply(items)
}

func handleHexists(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("hexists")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeHash); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	if connContext.Store.Hash.Exists(key, args[1].String) {
		return intReply(1)
	}
	return intReply(0)
}

func handleHlen(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("hlen")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeHash); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.Hash.FieldCount(key)))
}
