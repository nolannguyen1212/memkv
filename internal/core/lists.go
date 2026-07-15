package core

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
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
	if !connContext.Store.PrepareWrite(key, datastructure.KeyTypeList, false) {
		return wrongTypeError()
	}
	length := connContext.Store.List.LeftPush(key, values...)

	connContext.OnKeyWrite(key)
	if err := connContext.AppendAOF(buildBulkArray(append([]string{"LPUSH", key}, values...)...)); err != nil {
		return persistenceError()
	}

	return intReply(int64(length))
}

func handleRpush(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("rpush")
	}

	key := args[0].String
	values := extractStrings(args[1:])
	if !connContext.Store.PrepareWrite(key, datastructure.KeyTypeList, false) {
		return wrongTypeError()
	}
	length := connContext.Store.List.RightPush(key, values...)

	connContext.OnKeyWrite(key)
	if err := connContext.AppendAOF(buildBulkArray(append([]string{"RPUSH", key}, values...)...)); err != nil {
		return persistenceError()
	}

	return intReply(int64(length))
}

func handleLpop(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("lpop")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, datastructure.KeyTypeList); !valid {
		return wrongTypeError()
	}
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
		if connContext.Store.List.Length(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, datastructure.KeyTypeList)
		}
		if err := connContext.AppendAOF(buildBulkArray(append([]string{"LPOP"}, extractStrings(args)...)...)); err != nil {
			return persistenceError()
		}
	}
	return valuesToArray(values)
}

func handleRpop(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgCountError("rpop")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, datastructure.KeyTypeList); !valid {
		return wrongTypeError()
	}
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
		if connContext.Store.List.Length(key) == 0 {
			connContext.Store.RemoveEmptyKey(key, datastructure.KeyTypeList)
		}
		if err := connContext.AppendAOF(buildBulkArray(append([]string{"RPOP"}, extractStrings(args)...)...)); err != nil {
			return persistenceError()
		}
	}
	return valuesToArray(values)
}

func handleLlen(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("llen")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, datastructure.KeyTypeList); !valid {
		return wrongTypeError()
	}
	connContext.OnKeyRead(key)
	return intReply(int64(connContext.Store.List.Length(key)))
}

func handleLrange(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return wrongArgCountError("lrange")
	}

	key := args[0].String
	if _, valid := connContext.Store.HasType(key, datastructure.KeyTypeList); !valid {
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
	if exists, valid := connContext.Store.HasType(key, datastructure.KeyTypeList); !valid {
		return wrongTypeError()
	} else if !exists {
		return emptyArrayReply()
	}
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
	connContext.OnKeyMutate(key)
	if err := connContext.AppendAOF(buildBulkArray(append([]string{"SORT"}, extractStrings(args)...)...)); err != nil {
		return persistenceError()
	}
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
