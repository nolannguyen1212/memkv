package engine

import (
	"strconv"
	"strings"
	"time"

	"github.com/william1nguyen/memkv/internal/store"
)

func registerStringCommands(registry *Registry) {
	registry.register("SET", handleSet, syntax(2, 4, validateSetSyntax), writeCommand)
	registry.register("GET", handleGet, arguments(1, 1))
	registry.register("DEL", handleDel, arguments(1, -1), writeCommand)
	registry.register("EXPIRE", handleExpire, arguments(2, 2), writeCommand)
	registry.register("PEXPIREAT", handlePExpireAt, arguments(2, 2), writeCommand)
	registry.register("TTL", handleTTL, arguments(1, 1))
	registry.register("PING", handlePing, arguments(0, 1))
	registry.register("FLUSHDB", handleFlush, arguments(0, 0), writeCommand)
	registry.register("FLUSHALL", handleFlush, arguments(0, 0), writeCommand)
}

func handleSet(connContext *ConnContext, args []string) Result {
	if len(args) != 2 && len(args) != 4 {
		return wrongArgCountError("set")
	}

	key := args[0]
	value := args[1]
	var expiredAt time.Time

	if len(args) == 4 {
		amount, err := strconv.ParseInt(args[3], 10, 64)

		if err != nil {
			return notIntegerError()
		}

		switch strings.ToUpper(args[2]) {
		case "EX":
			var valid bool
			expiredAt, valid = relativeExpiration(connContext.Store.Now(), amount, time.Second)

			if !valid {
				return errorReply("ERR invalid expire time in 'set' command")
			}
		case "PX":
			var valid bool
			expiredAt, valid = relativeExpiration(connContext.Store.Now(), amount, time.Millisecond)

			if !valid {
				return errorReply("ERR invalid expire time in 'set' command")
			}
		case "EXAT":
			expiredAt = time.Unix(amount, 0)
		case "PXAT":
			expiredAt = time.UnixMilli(amount)
		default:
			return errorReply("ERR syntax error")
		}
	}

	record := newMutation("SET", key, value)

	if !expiredAt.IsZero() {
		record = newMutation("SET", key, value, "PXAT", strconv.FormatInt(expiredAt.UnixMilli(), 10))
	}

	if err := connContext.Commit(record, func() {
		connContext.Store.SetString(key, value, expiredAt)
	}); err != nil {
		return persistenceError()
	}

	return okReply()
}

func relativeExpiration(now time.Time, amount int64, unit time.Duration) (time.Time, bool) {
	if amount <= 0 || amount > int64((1<<63-1)/unit) {
		return time.Time{}, false
	}

	return now.Add(time.Duration(amount) * unit), true
}

func handleGet(connContext *ConnContext, args []string) Result {
	if len(args) < 1 {
		return wrongArgCountError("get")
	}

	key := args[0]

	if connContext.Store.CheckType(key, store.KeyTypeString) == store.StatusWrongType {
		return wrongTypeError()
	}

	value, exists := connContext.Store.Dictionary.Get(key)

	if !exists {
		return nullStringReply()
	}

	return stringReply(value)
}

func handleDel(connContext *ConnContext, args []string) Result {
	if len(args) < 1 {
		return wrongArgCountError("del")
	}

	keys := make([]string, 0, len(args))
	seen := make(map[string]struct{}, len(args))

	for _, arg := range args {
		key := arg

		if _, duplicate := seen[key]; duplicate {
			continue
		}

		seen[key] = struct{}{}

		if _, exists := connContext.Store.Keyspace.Type(key); exists {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return intReply(0)
	}

	record := newMutation(append([]string{"DEL"}, keys...)...)

	if err := connContext.Commit(record, func() {
		for _, key := range keys {
			connContext.Store.DeleteKey(key)
		}
	}); err != nil {
		return persistenceError()
	}

	return intReply(int64(len(keys)))
}

func handleExpire(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("expire")
	}

	key := args[0]
	seconds, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		return notIntegerError()
	}

	if _, exists := connContext.Store.Keyspace.Type(key); !exists {
		return intReply(0)
	}

	expiredAt := time.UnixMilli(0)

	if seconds > 0 {
		var valid bool
		expiredAt, valid = relativeExpiration(connContext.Store.Now(), seconds, time.Second)

		if !valid {
			return errorReply("ERR invalid expire time")
		}
	}

	record := newMutation("PEXPIREAT", key, strconv.FormatInt(expiredAt.UnixMilli(), 10))

	if err := connContext.Commit(record, func() {
		connContext.Store.Keyspace.ExpireAt(key, expiredAt)
	}); err != nil {
		return persistenceError()
	}

	return intReply(1)
}

func handlePExpireAt(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("pexpireat")
	}

	key := args[0]
	ms, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		return errorReply("ERR invalid expire time")
	}

	if _, exists := connContext.Store.Keyspace.Type(key); !exists {
		return intReply(0)
	}

	expiredAt := time.UnixMilli(ms)
	record := newMutation("PEXPIREAT", key, args[1])

	if err := connContext.Commit(record, func() {
		connContext.Store.Keyspace.ExpireAt(key, expiredAt)
	}); err != nil {
		return persistenceError()
	}

	return intReply(1)
}

func handleTTL(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("ttl")
	}

	return intReply(connContext.Store.Keyspace.TTL(args[0]))
}

func handlePing(connContext *ConnContext, args []string) Result {
	if len(args) == 0 {
		return pongReply()
	}

	return stringReply(args[0])
}

func handleFlush(connContext *ConnContext, _ []string) Result {
	state := connContext.Store.Snapshot()

	if len(state.Keyspace) == 0 {
		return okReply()
	}

	if err := connContext.Commit(newMutation("FLUSHALL"), func() {
		connContext.Store.Clear()
	}); err != nil {
		return persistenceError()
	}

	return okReply()
}
