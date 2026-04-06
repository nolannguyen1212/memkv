package core

import (
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	Register("ZADD", handleZadd)
	Register("ZREM", handleZrem)
	Register("ZSCORE", handleZscore)
	Register("ZRANK", handleZrank)
	Register("ZCARD", handleZcard)
	Register("ZRANGE", handleZrange)
}

func handleZadd(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgCountError("zadd")
	}

	key := args[0].String
	items := make([]datastructure.ScoreMember, 0, (len(args)-1)/2)

	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].String, 64)
		if err != nil {
			return errorReply("ERR value is not a valid float")
		}
		items = append(items, datastructure.ScoreMember{
			Score:  score,
			Member: args[i+1].String,
		})
	}

	count := connContext.Store.SortedSet.Add(key, items...)
	connContext.OnKeyWrite(key)
	return intReply(int64(count))
}

func handleZrem(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("zrem")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := connContext.Store.SortedSet.Remove(key, members...)

	connContext.OnKeyMutate(key)
	return intReply(int64(count))
}

func handleZscore(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("zscore")
	}

	connContext.OnKeyRead(args[0].String)
	score, exists := connContext.Store.SortedSet.Score(args[0].String, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return stringReply(strconv.FormatFloat(score, 'f', -1, 64))
}

func handleZrank(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("zrank")
	}

	connContext.OnKeyRead(args[0].String)
	rank, exists := connContext.Store.SortedSet.Rank(args[0].String, args[1].String)
	if !exists {
		return nullStringReply()
	}
	return intReply(int64(rank))
}

func handleZcard(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("zcard")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(int64(connContext.Store.SortedSet.Cardinality(args[0].String)))
}

func handleZrange(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return wrongArgCountError("zrange")
	}

	key := args[0].String
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
