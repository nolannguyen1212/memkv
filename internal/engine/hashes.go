package engine

import (
	"sort"

	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerHashCommands(registry *Registry) {
	registry.register("HSET", handleHset, syntax(3, -1, oddAfterKey), writeCommand)
	registry.register("HGET", handleHget, arguments(2, 2))
	registry.register("HDEL", handleHdel, arguments(2, -1), writeCommand)
	registry.register("HGETALL", handleHgetall, arguments(1, 1))
	registry.register("HEXISTS", handleHexists, arguments(2, 2))
	registry.register("HLEN", handleHlen, arguments(1, 1))
}

func handleHset(connContext *ConnContext, args []string) Result {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgCountError("hset")
	}

	key := args[0]
	fieldValues := extractStrings(args[1:])

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	count := connContext.Store.Hash.CountMissing(key, fieldValues...)

	if !connContext.Store.Hash.WouldChange(key, fieldValues...) {
		return intReply(int64(count))
	}

	record := newMutation(append([]string{"HSET", key}, fieldValues...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.Hash.Set(key, fieldValues...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleHget(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("hget")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	value, exists := connContext.Store.Hash.Get(key, args[1])

	if !exists {
		return nullStringReply()
	}

	return stringReply(value)
}

func handleHdel(connContext *ConnContext, args []string) Result {
	if len(args) < 2 {
		return wrongArgCountError("hdel")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	fields := extractStrings(args[1:])
	count := connContext.Store.Hash.CountExisting(key, fields...)

	if count == 0 {
		return intReply(0)
	}

	record := newMutation(append([]string{"HDEL", key}, fields...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.Hash.Delete(key, fields...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(count))
}

func handleHgetall(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("hgetall")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	fields, exists := connContext.Store.Hash.GetAll(key)

	if !exists {
		return emptyArrayReply()
	}

	names := make([]string, 0, len(fields))

	for field := range fields {
		names = append(names, field)
	}

	sort.Strings(names)
	items := make([]Result, 0, len(fields)*2)

	for _, field := range names {
		items = append(items, stringReply(field))
		items = append(items, stringReply(fields[field]))
	}

	return arrayReply(items)
}

func handleHexists(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("hexists")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	if connContext.Store.Hash.Exists(key, args[1]) {
		return intReply(1)
	}

	return intReply(0)
}

func handleHlen(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("hlen")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeHash) == store.StatusWrongType {
		return wrongTypeError()
	}

	return intReply(int64(connContext.Store.Hash.FieldCount(key)))
}
