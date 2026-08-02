package engine

import (
	"math"
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSortedSetCommands(registry *Registry) {
	registry.register("ZADD", handleZadd, syntax(3, -1, validateZAddSyntax), writeCommand)
	registry.register("ZREM", handleZrem, arguments(2, -1), writeCommand)
	registry.register("ZSCORE", handleZscore, arguments(2, 2))
	registry.register("ZRANK", handleZrank, arguments(2, 2))
	registry.register("ZCARD", handleZcard, arguments(1, 1))
	registry.register("ZRANGE", handleZrange, arguments(3, 3))
}

func handleZadd(connContext *ConnContext, args []string) Result {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgCountError("zadd")
	}

	key := args[0]
	items := make([]store.ScoreMember, 0, (len(args)-1)/2)

	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil || math.IsNaN(score) {
			return errorReply("ERR value is not a valid float")
		}
		items = append(items, store.ScoreMember{
			Score:  score,
			Member: args[i+1],
		})
	}

	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	count := connContext.Store.SortedSet.CountMissing(key, items...)
	if !connContext.Store.SortedSet.WouldChange(key, items...) {
		return intReply(int64(count))
	}
	record := newMutation(append([]string{"ZADD"}, extractStrings(args)...)...)
	if err := connContext.Commit(record, func() {
		connContext.Store.PrepareWrite(key, store.KeyTypeSortedSet, false)
		connContext.Store.SortedSet.Add(key, items...)
		connContext.OnKeyWrite(key)
	}); err != nil {
		return persistenceError()
	}
	return intReply(int64(count))
}

func handleZrem(connContext *ConnContext, args []string) Result {
	if len(args) < 2 {
		return wrongArgCountError("zrem")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	members := extractStrings(args[1:])
	count := connContext.Store.SortedSet.CountExisting(key, members...)
	if count == 0 {
		return intReply(0)
	}
	record := newMutation(append([]string{"ZREM"}, extractStrings(args)...)...)
	if err := connContext.Commit(record, func() {
		connContext.Store.SortedSet.Remove(key, members...)
		connContext.OnKeyMutate(key)
		if connContext.Store.SortedSet.Cardinality(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, store.KeyTypeSortedSet)
		}
	}); err != nil {
		return persistenceError()
	}
	return intReply(int64(count))
}

func handleZscore(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("zscore")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	score, exists := connContext.Store.SortedSet.Score(key, args[1])
	if !exists {
		return nullStringReply()
	}
	return stringReply(strconv.FormatFloat(score, 'f', -1, 64))
}

func handleZrank(connContext *ConnContext, args []string) Result {
	if len(args) != 2 {
		return wrongArgCountError("zrank")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	rank, exists := connContext.Store.SortedSet.Rank(key, args[1])
	if !exists {
		return nullStringReply()
	}
	return intReply(int64(rank))
}

func handleZcard(connContext *ConnContext, args []string) Result {
	if len(args) != 1 {
		return wrongArgCountError("zcard")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.SortedSet.Cardinality(key)))
}

func handleZrange(connContext *ConnContext, args []string) Result {
	if len(args) != 3 {
		return wrongArgCountError("zrange")
	}

	key := args[0]
	if connContext.Store.CheckType(key, store.KeyTypeSortedSet) == store.StatusWrongType {
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

	connContext.OnKeyRead(key)
	members := connContext.Store.SortedSet.Range(key, start, stop)
	return valuesToArray(members)
}
