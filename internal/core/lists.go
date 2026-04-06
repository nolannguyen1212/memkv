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

func handleLpush(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("lpush")
	}

	key := args[0].String
	values := extractStrings(args[1:])
	length := connContext.Store.List.LeftPush(key, values...)

	connContext.OnKeyWrite(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"LPUSH", key}, values...)...))

	return intReply(int64(length))
}

func handleRpush(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("rpush")
	}

	key := args[0].String
	values := extractStrings(args[1:])
	length := connContext.Store.List.RightPush(key, values...)

	connContext.OnKeyWrite(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"RPUSH", key}, values...)...))

	return intReply(int64(length))
}

func handleLpop(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("lpop")
	}

	key := args[0].String
	count := 1
	if len(args) > 1 {
		parsedCount, err := strconv.Atoi(args[1].String)
		if err != nil {
			return notIntegerError()
		}
		count = parsedCount
	}

	values := connContext.Store.List.LeftPop(key, count)
	if len(values) > 0 {
		connContext.OnKeyMutate(key)
	}
	return valuesToArray(values)
}

func handleRpop(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("rpop")
	}

	key := args[0].String
	count := 1
	if len(args) > 1 {
		parsedCount, err := strconv.Atoi(args[1].String)
		if err != nil {
			return notIntegerError()
		}
		count = parsedCount
	}

	values := connContext.Store.List.RightPop(key, count)
	if len(values) > 0 {
		connContext.OnKeyMutate(key)
	}
	return valuesToArray(values)
}

func handleLlen(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("llen")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(int64(connContext.Store.List.Length(args[0].String)))
}

func handleLrange(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return wrongArgCountError("lrange")
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
	values, exists := connContext.Store.List.Range(key, start, stop)
	if !exists {
		return nullArrayReply()
	}
	return valuesToArray(values)
}

func handleSort(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgCountError("sort")
	}

	key := args[0].String
	ascending := true
	alpha := false

	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i].String) {
		case "ASC":
			ascending = true
		case "DESC":
			ascending = false
		case "ALPHA":
			alpha = true
		default:
			return syntaxError()
		}
	}

	connContext.Store.List.Sort(key, ascending, alpha)
	return okReply()
}

// valuesToArray converts a string slice to a RESP array reply.
func valuesToArray(values []string) protocol.Value {
	items := make([]protocol.Value, len(values))
	for i, v := range values {
		items[i] = stringReply(v)
	}
	return arrayReply(items)
}
