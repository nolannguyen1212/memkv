package engine

import (
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerListCommands(registry *Registry) {
	registry.register("LPUSH", handleLpush, arguments(2, -1), writeCommand)
	registry.register("RPUSH", handleRpush, arguments(2, -1), writeCommand)
	registry.register("LPOP", handleLpop, arguments(1, 2), writeCommand)
	registry.register("RPOP", handleRpop, arguments(1, 2), writeCommand)
	registry.register("LLEN", handleLlen, arguments(1, 1))
	registry.register("LRANGE", handleLrange, arguments(3, 3))
}

func handleLpush(connContext *ConnContext, args []string) Result {
	if len(args) < 2 {
		return wrongArgCountError("lpush")
	}

	key := args[0]
	values := extractStrings(args[1:])

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	length := connContext.Store.List.Length(key) + len(values)
	record := newMutation(append([]string{"LPUSH", key}, values...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.List.LeftPush(key, values...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(length))
}

func handleRpush(connContext *ConnContext, args []string) Result {
	if len(args) < 2 {
		return wrongArgCountError("rpush")
	}

	key := args[0]
	values := extractStrings(args[1:])

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	length := connContext.Store.List.Length(key) + len(values)
	record := newMutation(append([]string{"RPUSH", key}, values...)...)

	if err := connContext.Commit(record, func() {
		connContext.Store.List.RightPush(key, values...)
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(length))
}

func handleLpop(connContext *ConnContext, args []string) Result {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("lpop")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	count := 1

	if len(args) > 1 {
		parsedCount, err := strconv.Atoi(args[1])

		if err != nil {
			return notIntegerError()
		}

		if parsedCount < 0 {
			return errorReply("ERR count must be non-negative")
		}

		count = parsedCount
	}

	values := connContext.Store.List.PeekLeft(key, count)

	if len(values) > 0 {
		record := newMutation(append([]string{"LPOP"}, extractStrings(args)...)...)

		if err := connContext.Commit(record, func() {
			connContext.Store.List.LeftPop(key, count)
		}); err != nil {
			return persistenceError()
		}
	}

	return popReply(values, len(args) == 2)
}

func handleRpop(connContext *ConnContext, args []string) Result {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("rpop")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	count := 1

	if len(args) > 1 {
		parsedCount, err := strconv.Atoi(args[1])

		if err != nil {
			return notIntegerError()
		}

		if parsedCount < 0 {
			return errorReply("ERR count must be non-negative")
		}

		count = parsedCount
	}

	values := connContext.Store.List.PeekRight(key, count)

	if len(values) > 0 {
		record := newMutation(append([]string{"RPOP"}, extractStrings(args)...)...)

		if err := connContext.Commit(record, func() {
			connContext.Store.List.RightPop(key, count)
		}); err != nil {
			return persistenceError()
		}
	}

	return popReply(values, len(args) == 2)
}

func handleLlen(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("llen")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	return intReply(int64(connContext.Store.List.Length(key)))
}

func handleLrange(connContext *ConnContext, args []string) Result {
	if len(args) != 3 {
		return wrongArgCountError("lrange")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeList) == store.StatusWrongType {
		return wrongTypeError()
	}

	start, err := strconv.Atoi(args[1])

	if err != nil {
		return notIntegerError()
	}

	stop, err := strconv.Atoi(args[2])

	if err != nil {
		return notIntegerError()
	}

	values, exists := connContext.Store.List.Range(key, start, stop)

	if !exists {
		return emptyArrayReply()
	}

	return valuesToArray(values)
}

func popReply(values []string, countProvided bool) Result {
	if countProvided {
		return valuesToArray(values)
	}

	if len(values) == 0 {
		return nullStringReply()
	}

	return stringReply(values[0])
}

func valuesToArray(values []string) Result {
	items := make([]Result, len(values))

	for i, v := range values {
		items[i] = stringReply(v)
	}

	return arrayReply(items)
}
