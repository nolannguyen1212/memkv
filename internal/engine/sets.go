package engine

import (
	"sort"

	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSetCommands(registry *Registry) {
	registry.register("SADD", handleSadd, arguments(2, -1), writeCommand)
	registry.register("SREM", handleSrem, arguments(2, -1), writeCommand)
	registry.register("SMEMBERS", handleSmembers, arguments(1, 1))
	registry.register("SISMEMBER", handleSismember, arguments(2, 2))
	registry.register("SCARD", handleScard, arguments(1, 1))
}

func handleSadd(connContext *ConnContext, args []string) Result {
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

	record := newMutation(append([]string{"SADD", key}, members...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.Set.Add(key, members...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleSrem(connContext *ConnContext, args []string) Result {
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

	record := newMutation(append([]string{"SREM", key}, members...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.Set.Remove(key, members...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleSmembers(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("smembers")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}

	members, exists := connContext.Store.Set.Members(key)

	if !exists {
		return emptyArrayReply()
	}

	sort.Strings(members)

	items := make([]Result, len(members))

	for i, member := range members {
		items[i] = stringReply(member)
	}

	return arrayReply(items)
}

func handleSismember(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("sismember")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}

	if connContext.Store.Set.IsMember(key, args[1]) {
		return intReply(1)
	}

	return intReply(0)
}

func handleScard(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("scard")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeSet) == store.StatusWrongType {
		return wrongTypeError()
	}

	return intReply(int64(connContext.Store.Set.Cardinality(key)))
}

func extractStrings(args []string) []string {
	result := make([]string, len(args))
	copy(result, args)
	return result
}
