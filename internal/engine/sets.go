package engine

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSetCommands(registry *Registry) {
	registry.Register("SADD", handleSadd)
	registry.Register("SREM", handleSrem)
	registry.Register("SMEMBERS", handleSmembers)
	registry.Register("SISMEMBER", handleSismember)
	registry.Register("SCARD", handleScard)
	registry.Register("SEXPIRE", handleSexpire)
	registry.Register("STTL", handleSttl)
}

func handleSadd(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("sadd")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	if !connContext.Store.PrepareWrite(key, store.KeyTypeSet, false) {
		return wrongTypeError()
	}
	count := connContext.Store.Set.Add(key, members...)

	connContext.OnKeyWrite(key)
	if err := connContext.AppendWAL(buildBulkArray(append([]string{"SADD", key}, members...)...)); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleSrem(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("srem")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	}
	members := extractStrings(args[1:])
	count := connContext.Store.Set.Remove(key, members...)

	if count > 0 {
		connContext.OnKeyMutate(key)
		if connContext.Store.Set.Cardinality(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, store.KeyTypeSet)
		}
		if err := connContext.AppendWAL(buildBulkArray(append([]string{"SREM", key}, members...)...)); err != nil {
			return persistenceError()
		}
	}

	return intReply(int64(count))
}

func handleSmembers(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("smembers")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	members, exists := connContext.Store.Set.Members(key)
	if !exists {
		return nullArrayReply()
	}

	items := make([]resp.Value, len(members))
	for i, member := range members {
		items[i] = stringReply(member)
	}
	return arrayReply(items)
}

func handleSismember(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("sismember")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	if connContext.Store.Set.IsMember(key, args[1].String) {
		return intReply(1)
	}
	return intReply(0)
}

func handleScard(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("scard")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.Set.Cardinality(key)))
}

func handleSexpire(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("sexpire")
	}

	key := args[0].String
	if exists, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	} else if !exists {
		return intReply(0)
	}
	seconds, err := strconv.Atoi(args[1].String)
	if err != nil {
		return notIntegerError()
	}

	expAt := time.Now().Add(time.Duration(seconds) * time.Second)
	if !connContext.Store.Keyspace.ExpireAt(key, expAt) {
		return intReply(0)
	}
	connContext.OnKeyMutate(key)
	if err := connContext.AppendWAL(buildBulkArray("PEXPIREAT", key, strconv.FormatInt(expAt.UnixMilli(), 10))); err != nil {
		return persistenceError()
	}
	return intReply(1)
}

func handleSttl(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("sttl")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(connContext.Store.Keyspace.TTL(key))
}

// extractStrings converts a slice of resp.Value to a slice of strings.
func extractStrings(args []resp.Value) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = arg.String
	}
	return result
}
