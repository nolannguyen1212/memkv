package core

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	Register("LPUSH", handleLpush)
	Register("RPUSH", handleRpush)
	Register("LPOP", handleLpop)
	Register("RPOP", handleRpop)
	Register("LLEN", handleLlen)
	Register("LRANGE", handleLrange)
	Register("SORT", handleSort)
}

func handleLpush(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("lpush")
	}

	key := args[0].String
	values := extractStrings(args[1:])
	length := cctx.Store.List.LeftPush(key, values...)

	cctx.OnKeyWrite(key)
	cctx.AppendAOF(BuildBulkArray(append([]string{"LPUSH", key}, values...)...))

	return IntegerResponse(int64(length))
}

func handleRpush(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return WrongArgCountError("rpush")
	}

	key := args[0].String
	values := extractStrings(args[1:])
	length := cctx.Store.List.RightPush(key, values...)

	cctx.OnKeyWrite(key)
	cctx.AppendAOF(BuildBulkArray(append([]string{"RPUSH", key}, values...)...))

	return IntegerResponse(int64(length))
}

func handleLpop(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgCountError("lpop")
	}

	key := args[0].String
	count := 1
	if len(args) > 1 {
		c, err := strconv.Atoi(args[1].String)
		if err != nil {
			return NotIntegerError()
		}
		count = c
	}

	values := cctx.Store.List.LeftPop(key, count)
	if len(values) > 0 {
		cctx.OnKeyMutate(key)
	}
	return stringsToArray(values)
}

func handleRpop(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgCountError("rpop")
	}

	key := args[0].String
	count := 1
	if len(args) > 1 {
		c, err := strconv.Atoi(args[1].String)
		if err != nil {
			return NotIntegerError()
		}
		count = c
	}

	values := cctx.Store.List.RightPop(key, count)
	if len(values) > 0 {
		cctx.OnKeyMutate(key)
	}
	return stringsToArray(values)
}

func handleLlen(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("llen")
	}

	cctx.OnKeyRead(args[0].String)
	return IntegerResponse(int64(cctx.Store.List.Length(args[0].String)))
}

func handleLrange(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return WrongArgCountError("lrange")
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
	values, exists := cctx.Store.List.Range(key, start, stop)
	if !exists {
		return NullArrayResponse()
	}
	return stringsToArray(values)
}

func handleSort(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("sort")
	}

	key := args[0].String
	ascending := true
	alpha := false

	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(args[i].String)
		switch opt {
		case "ASC":
			ascending = true
		case "DESC":
			ascending = false
		case "ALPHA":
			alpha = true
		default:
			return SyntaxError()
		}
	}

	cctx.Store.List.Sort(key, ascending, alpha)
	return OKResponse()
}

func stringsToArray(values []string) protocol.Value {
	items := make([]protocol.Value, len(values))
	for i, v := range values {
		items[i] = StringResponse(v)
	}
	return ArrayResponse(items)
}
