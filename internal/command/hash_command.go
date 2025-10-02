package command

import (
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type HashStore interface {
	Hset(key string, fieldValues ...string) int
	Hget(key, field string) (string, bool)
	Hdel(key string, fields ...string) int
	Hgetall(key string) (map[string]string, bool)
	Hexists(key, field string) bool
	Hlen(key string) int
	Dump() map[string]map[string]string
}

type HashContext struct {
	Hash *datastructure.HashMap
	AOF  *persistence.AOF
}

var hashCtx *HashContext

func SetHashContext(c *HashContext) { hashCtx = c }

func InitHashCommands() {
	Register("HSET", cmdHset)
	Register("HGET", cmdHget)
	Register("HDEL", cmdHdel)
	Register("HGETALL", cmdHgetall)
	Register("HEXISTS", cmdHexists)
	Register("HLEN", cmdHlen)
}

func cmdHset(args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hset'"}
	}
	key := args[0].Text
	fieldValues := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		fieldValues = append(fieldValues, a.Text)
	}
	n := hashCtx.Hash.Hset(key, fieldValues...)
	if hashCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "HSET"}, {Type: resp.BulkString, Text: key}}
		for _, fv := range fieldValues {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: fv})
		}
		_ = hashCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdHget(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hget'"}
	}
	key := args[0].Text
	field := args[1].Text
	val, ok := hashCtx.Hash.Hget(key, field)
	if !ok {
		return resp.Value{Type: resp.BulkString, IsNil: true}
	}
	return resp.Value{Type: resp.BulkString, Text: val}
}

func cmdHdel(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hdel'"}
	}
	key := args[0].Text
	fields := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		fields = append(fields, a.Text)
	}
	n := hashCtx.Hash.Hdel(key, fields...)
	if hashCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "HDEL"}, {Type: resp.BulkString, Text: key}}
		for _, f := range fields {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: f})
		}
		_ = hashCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdHgetall(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hgetall'"}
	}
	key := args[0].Text
	hash, ok := hashCtx.Hash.Hgetall(key)
	if !ok {
		return resp.Value{Type: resp.Array, Items: []resp.Value{}}
	}
	items := make([]resp.Value, 0, len(hash)*2)
	for field, value := range hash {
		items = append(items, resp.Value{Type: resp.BulkString, Text: field})
		items = append(items, resp.Value{Type: resp.BulkString, Text: value})
	}
	return resp.Value{Type: resp.Array, Items: items}
}

func cmdHexists(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hexists'"}
	}
	key := args[0].Text
	field := args[1].Text
	if hashCtx.Hash.Hexists(key, field) {
		return resp.Value{Type: resp.Integer, Number: 1}
	}
	return resp.Value{Type: resp.Integer, Number: 0}
}

func cmdHlen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'hlen'"}
	}
	key := args[0].Text
	return resp.Value{Type: resp.Integer, Number: int64(hashCtx.Hash.Hlen(key))}
}
