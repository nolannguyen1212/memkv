package command

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type DictStore interface {
	Set(key, value string, ttl time.Duration)
	Get(key string) (string, bool)
	Delete(keys ...string) int
	Expire(key string, ttl time.Duration) bool
	ExpireAt(key string, at time.Time) bool
	TTL(key string) int64
	Dump() map[string]datastructure.Item
}

type DictContext struct {
	Dict DictStore
	AOF  *persistence.AOF
}

var ctx *DictContext

func SetDictContext(c *DictContext) { ctx = c }

func InitDictCommands() {
	Register("SET", cmdSet)
	Register("GET", cmdGet)
	Register("DEL", cmdDel)
	Register("EXPIRE", cmdExpire)
	Register("TTL", cmdTTL)
	Register("PING", cmdPing)
	Register("PEXPIREAT", cmdPExpireAt)
}

func cmdSet(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'set'"}
	}
	key := args[0].Text
	val := args[1].Text
	var ttl time.Duration
	if len(args) > 2 {
		if seconds, err := strconv.Atoi(args[2].Text); err == nil && seconds > 0 {
			ttl = time.Duration(seconds) * time.Second
		}
	}
	ctx.Dict.Set(key, val, ttl)
	if ctx.AOF != nil {
		_ = ctx.AOF.Append(resp.Value{
			Type: resp.Array,
			Items: []resp.Value{
				{Type: resp.BulkString, Text: "SET"},
				{Type: resp.BulkString, Text: key},
				{Type: resp.BulkString, Text: val},
			},
		})
		if ttl > 0 {
			at := time.Now().Add(ttl)
			_ = ctx.AOF.Append(resp.Value{
				Type: resp.Array,
				Items: []resp.Value{
					{Type: resp.BulkString, Text: "PEXPIREAT"},
					{Type: resp.BulkString, Text: key},
					{Type: resp.BulkString, Text: strconv.FormatInt(at.UnixMilli(), 10)},
				},
			})
		}
	}
	return resp.Value{Type: resp.SimpleString, Text: "OK"}
}

func cmdGet(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'get'"}
	}
	key := args[0].Text
	val, ok := ctx.Dict.Get(key)
	if !ok {
		return resp.Value{Type: resp.BulkString, IsNil: true}
	}
	return resp.Value{Type: resp.BulkString, Text: val}
}

func cmdDel(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'del'"}
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Text
	}
	n := ctx.Dict.Delete(keys...)
	if ctx.AOF != nil {
		arr := []resp.Value{{Type: resp.BulkString, Text: "DEL"}}
		for _, k := range keys {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: k})
		}
		_ = ctx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'expire'"}
	}
	key := args[0].Text
	seconds, err := strconv.Atoi(args[1].Text)
	if err != nil {
		return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
	}
	ok := ctx.Dict.Expire(key, time.Duration(seconds)*time.Second)
	at := time.Now().Add(time.Duration(seconds) * time.Second)
	if !ok {
		return resp.Value{Type: resp.Integer, Number: 0}
	}
	if ctx.AOF != nil {
		_ = ctx.AOF.Append(resp.Value{
			Type: resp.Array,
			Items: []resp.Value{
				{Type: resp.BulkString, Text: "PEXPIREAT"},
				{Type: resp.BulkString, Text: key},
				{Type: resp.BulkString, Text: strconv.FormatInt(at.UnixMilli(), 10)},
			},
		})
	}
	return resp.Value{Type: resp.Integer, Number: 1}
}

func cmdPExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'pexpireat'"}
	}
	key := args[0].Text
	ms, err := strconv.ParseInt(args[1].Text, 10, 64)
	if err != nil {
		return resp.Value{Type: resp.Error, Text: "ERR invalid expire time"}
	}
	at := time.UnixMilli(ms)
	ok := ctx.Dict.ExpireAt(key, at)
	if !ok {
		return resp.Value{Type: resp.Integer, Number: 0}
	}
	return resp.Value{Type: resp.Integer, Number: 1}
}

func cmdTTL(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'ttl'"}
	}
	key := args[0].Text
	remaining := ctx.Dict.TTL(key)
	return resp.Value{Type: resp.Integer, Number: remaining}
}

func cmdPing(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Type: resp.SimpleString, Text: "PONG"}
	}
	return resp.Value{Type: resp.BulkString, Text: args[0].Text}
}
