package engine

import (
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSortedSetCommands(registry *Registry) {
	registry.Register("ZADD", handleZadd)
	registry.Register("ZREM", handleZrem)
	registry.Register("ZSCORE", handleZscore)
	registry.Register("ZRANK", handleZrank)
	registry.Register("ZCARD", handleZcard)
	registry.Register("ZRANGE", handleZrange)
}

func handleZadd(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgCountError("zadd")
	}

	key := args[0].String
	items := make([]store.ScoreMember, 0, (len(args)-1)/2)

	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].String, 64)
		if err != nil {
			return errorReply("ERR value is not a valid float")
		}
		items = append(items, store.ScoreMember{
			Score:  score,
			Member: args[i+1].String,
		})
	}

	if !connContext.Store.PrepareWrite(key, store.KeyTypeSortedSet, false) {
		return wrongTypeError()
	}
	count := connContext.Store.SortedSet.Add(key, items...)
	connContext.OnKeyWrite(key)
	if err := connContext.AppendWAL(buildBulkArray(append([]string{"ZADD"}, extractStrings(args)...)...)); err != nil {
		return persistenceError()
	}
	return intReply(int64(count))
}

func handleZrem(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgCountError("zrem")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSortedSet); !valid {
		return wrongTypeError()
	}
	members := extractStrings(args[1:])
	count := connContext.Store.SortedSet.Remove(key, members...)

	if count > 0 {
		connContext.OnKeyMutate(key)
		if connContext.Store.SortedSet.Cardinality(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, store.KeyTypeSortedSet)
		}
		if err := connContext.AppendWAL(buildBulkArray(append([]string{"ZREM"}, extractStrings(args)...)...)); err != nil {
			return persistenceError()
		}
	}
	return intReply(int64(count))
}

func handleZscore(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("zscore")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSortedSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	score, exists := connContext.Store.SortedSet.Score(key, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return stringReply(strconv.FormatFloat(score, 'f', -1, 64))
}

func handleZrank(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("zrank")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSortedSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	rank, exists := connContext.Store.SortedSet.Rank(key, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return intReply(int64(rank))
}

func handleZcard(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("zcard")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSortedSet); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.SortedSet.Cardinality(key)))
}

func handleZrange(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return wrongArgCountError("zrange")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, store.KeyTypeSortedSet); !valid {
		return wrongTypeError()
	}
	start, err := strconv.Atoi(args[1].String)
	if err != nil {
		return notIntegerError()
	}
	stop, err := strconv.Atoi(args[2].String)
	if err != nil {
		return notIntegerError()
	}

	connContext.OnKeyRead(key)
	members := connContext.Store.SortedSet.Range(key, start, stop)
	return valuesToArray(members)
}
