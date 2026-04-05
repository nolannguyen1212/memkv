package core

import (
	"strconv"

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

func handleZadd(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return WrongArgCountError("zadd")
	}

	key := args[0].String
	scoreMembers := make([]interface{}, 0, len(args)-1)

	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].String, 64)
		if err != nil {
			return ErrorResponse("ERR value is not a valid float")
		}
		member := args[i+1].String
		scoreMembers = append(scoreMembers, score, member)
	}

	count := cctx.Store.SortedList.Add(key, scoreMembers...)

	cctx.OnKeyWrite(key)

	return IntegerResponse(int64(count))
}

func handleZrem(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("zrem")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := cctx.Store.SortedList.Remove(key, members...)

	cctx.OnKeyMutate(key)
	return IntegerResponse(int64(count))
}

func handleZscore(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("zscore")
	}

	cctx.OnKeyRead(args[0].String)
	score, exists := cctx.Store.SortedList.Score(args[0].String, args[1].String)
	if !exists {
		return NullStringResponse()
	}
	return StringResponse(strconv.FormatFloat(score, 'f', -1, 64))
}

func handleZrank(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("zrank")
	}

	cctx.OnKeyRead(args[0].String)
	rank, exists := cctx.Store.SortedList.Rank(args[0].String, args[1].String)
	if !exists {
		return NullStringResponse()
	}
	return IntegerResponse(int64(rank))
}

func handleZcard(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("zcard")
	}

	cctx.OnKeyRead(args[0].String)
	return IntegerResponse(int64(cctx.Store.SortedList.Cardinality(args[0].String)))
}

func handleZrange(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return WrongArgCountError("zrange")
	}

	key := args[0].String
	start, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NotIntegerError()
	}
	stop, err := strconv.Atoi(args[2].String)
	if err != nil {
		return NotIntegerError()
	}

	cctx.OnKeyRead(key)
	members := cctx.Store.SortedList.Range(key, start, stop)
	return stringsToArray(members)
}
