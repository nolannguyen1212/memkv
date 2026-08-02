package engine

import (
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSetCommands(registry *Registry) {
	registry.Register("SADD", handleSadd)
	registry.Register("SREM", handleSrem)
	registry.Register("SMEMBERS", handleSmembers)
	registry.Register("SISMEMBER", handleSismember)
	registry.Register("SCARD", handleScard)
}

func handleSadd(connContext *ConnContext, args []string) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("sadd")
	}

	key := args[0]
	members := extractStrings(args[1:])
	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	count := connContext.Store.Set.CountMissing(key, members...)
	if count == 0 {
		return intReply(0)
	}
	record := buildBulkArray(append([]string{"SADD", key}, members...)...)
	if err := connContext.Commit(record, func() {
		connContext.Store.PrepareWrite(key, store.KeyTypeSet, false)
		connContext.Store.Set.Add(key, members...)
		connContext.OnKeyWrite(key)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleSrem(connContext *ConnContext, args []string) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("srem")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	members := extractStrings(args[1:])
	count := connContext.Store.Set.CountExisting(key, members...)
	if count == 0 {
		return intReply(0)
	}
	record := buildBulkArray(append([]string{"SREM", key}, members...)...)
	if err := connContext.Commit(record, func() {
		connContext.Store.Set.Remove(key, members...)
		connContext.OnKeyMutate(key)
		if connContext.Store.Set.Cardinality(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, store.KeyTypeSet)
		}
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleSmembers(connContext *ConnContext, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("smembers")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
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

func handleSismember(connContext *ConnContext, args []string) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("sismember")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	if connContext.Store.Set.IsMember(key, args[1]) {
		return intReply(1)
	}
	return intReply(0)
}

func handleScard(connContext *ConnContext, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("scard")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.Set.Cardinality(key)))
}

func extractStrings(args []string) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = arg
	}
	return result
}
