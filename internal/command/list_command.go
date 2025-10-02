package command

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type ListStore interface {
	Lpush(key string, values ...string) int
	Rpush(key string, values ...string) int
	Lpop(key string, count int) []datastructure.Item
	Rpop(key string, count int) []datastructure.Item
	Llen(key string) int
	Lrange(key string, start int, stop int) ([]datastructure.Item, bool)
	Sort(key string, asc bool, alpha bool)
}

type ListContext struct {
	List ListStore
	AOF  *persistence.AOF
}

var listCtx *ListContext

func SetListContext(c *ListContext) { listCtx = c }

func InitListCommands() {
	Register("LPUSH", cmdLpush)
	Register("RPUSH", cmdRpush)
	Register("LPOP", cmdLpop)
	Register("RPOP", cmdRpop)
	Register("LLEN", cmdLlen)
	Register("LRANGE", cmdLrange)
	Register("SORT", cmdSort)
}

func cmdLpush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'lpush'",
		}
	}
	key := args[0].Text
	values := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		values = append(values, a.Text)
	}
	n := listCtx.List.Lpush(key, values...)
	if listCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "LPUSH"}, {Type: resp.BulkString, Text: key}}
		for _, v := range values {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: v})
		}
		_ = listCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdRpush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'rpush'",
		}
	}
	key := args[0].Text
	values := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		values = append(values, a.Text)
	}
	n := listCtx.List.Rpush(key, values...)
	if listCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "RPUSH"}, {Type: resp.BulkString, Text: key}}
		for _, v := range values {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: v})
		}
		_ = listCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdLpop(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'lpop'",
		}
	}
	key := args[0].Text
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].Text)
		if err != nil {
			return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
		}
	}
	members := listCtx.List.Lpop(key, count)
	items := make([]resp.Value, 0, len(members))
	for _, m := range members {
		items = append(items, resp.Value{Type: resp.BulkString, Text: m.Value})
	}
	return resp.Value{
		Type:  resp.Array,
		Items: items,
	}
}

func cmdRpop(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'rpop'",
		}
	}
	key := args[0].Text
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].Text)
		if err != nil {
			return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
		}
	}
	members := listCtx.List.Rpop(key, count)
	items := make([]resp.Value, 0, len(members))
	for _, m := range members {
		items = append(items, resp.Value{Type: resp.BulkString, Text: m.Value})
	}
	return resp.Value{
		Type:  resp.Array,
		Items: items,
	}
}

func cmdLlen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'llen'",
		}
	}
	key := args[0].Text
	return resp.Value{Type: resp.Integer, Number: int64(listCtx.List.Llen(key))}
}

func cmdLrange(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'lrange'",
		}
	}
	key := args[0].Text
	start, err := strconv.Atoi(args[1].Text)
	if err != nil {
		return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
	}
	stop, err := strconv.Atoi(args[2].Text)
	if err != nil {
		return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
	}

	members, ok := listCtx.List.Lrange(key, start, stop)

	if !ok {
		return resp.Value{
			Type:  resp.Array,
			IsNil: true,
		}
	}

	items := make([]resp.Value, 0, len(members))
	for _, m := range members {
		items = append(items, resp.Value{Type: resp.BulkString, Text: m.Value})
	}
	return resp.Value{
		Type:  resp.Array,
		Items: items,
	}
}

func cmdSort(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'sort'",
		}
	}
	key := args[0].Text
	asc := true
	alpha := false
	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			option := strings.ToUpper(args[i].Text)
			switch option {
			case "ASC":
				asc = true
			case "DESC":
				asc = false
			case "ALPHA":
				alpha = true
			default:
				return resp.Value{
					Type: resp.Error,
					Text: "ERR syntax error",
				}
			}
		}
	}
	listCtx.List.Sort(key, asc, alpha)
	if listCtx.AOF != nil {
		arr := []resp.Value{{Type: resp.BulkString, Text: "SORT"}, {Type: resp.BulkString, Text: key}}
		for i := 1; i < len(args); i++ {
			arr = append(arr, args[i])
		}
		_ = listCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{
		Type: resp.SimpleString,
		Text: "OK",
	}
}
