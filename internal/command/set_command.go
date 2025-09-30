package command

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/persistence"
	resp "github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type SetStore interface {
	Sadd(key string, members ...string) int
	Srem(key string, members ...string) int
	Smembers(key string) ([]string, bool)
	Sismember(key, member string) bool
	Scard(key string) int
	Expire(key string, ttl time.Duration) bool
	TTL(key string) int64
}

type SetContext struct {
	Set SetStore
	AOF *persistence.AOF
}

var setCtx *SetContext

func SetSetContext(c *SetContext) { setCtx = c }

func InitSetCommands() {
	Register("SADD", cmdSAdd)
	Register("SREM", cmdSRem)
	Register("SMEMBERS", cmdSMembers)
	Register("SISMEMBER", cmdSIsMember)
	Register("SCARD", cmdSCard)
	Register("SEXPIRE", cmdSExpire)
	Register("STTL", cmdSTTL)
}

func cmdSAdd(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'sadd'"}
	}
	key := args[0].Text
	members := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		members = append(members, a.Text)
	}
	n := setCtx.Set.Sadd(key, members...)
	if setCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "SADD"}, {Type: resp.BulkString, Text: key}}
		for _, m := range members {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: m})
		}
		_ = setCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdSRem(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'srem'"}
	}
	key := args[0].Text
	members := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		members = append(members, a.Text)
	}
	n := setCtx.Set.Srem(key, members...)
	if setCtx.AOF != nil && n > 0 {
		arr := []resp.Value{{Type: resp.BulkString, Text: "SREM"}, {Type: resp.BulkString, Text: key}}
		for _, m := range members {
			arr = append(arr, resp.Value{Type: resp.BulkString, Text: m})
		}
		_ = setCtx.AOF.Append(resp.Value{Type: resp.Array, Items: arr})
	}
	return resp.Value{Type: resp.Integer, Number: int64(n)}
}

func cmdSMembers(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'smembers'"}
	}
	key := args[0].Text
	members, ok := setCtx.Set.Smembers(key)
	if !ok {
		return resp.Value{Type: resp.Array, IsNil: true}
	}
	items := make([]resp.Value, 0, len(members))
	for _, m := range members {
		items = append(items, resp.Value{Type: resp.BulkString, Text: m})
	}
	return resp.Value{Type: resp.Array, Items: items}
}

func cmdSIsMember(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'sismember'"}
	}
	key := args[0].Text
	member := args[1].Text
	if setCtx.Set.Sismember(key, member) {
		return resp.Value{Type: resp.Integer, Number: 1}
	}
	return resp.Value{Type: resp.Integer, Number: 0}
}

func cmdSCard(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'scard'"}
	}
	key := args[0].Text
	return resp.Value{Type: resp.Integer, Number: int64(setCtx.Set.Scard(key))}
}

func cmdSExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'sexpire'"}
	}
	key := args[0].Text
	seconds, err := strconv.Atoi(args[1].Text)
	if err != nil {
		return resp.Value{Type: resp.Error, Text: "ERR value is not an integer or out of range"}
	}
	ok := setCtx.Set.Expire(key, time.Duration(seconds)*time.Second)
	at := time.Now().Add(time.Duration(seconds) * time.Second)
	if !ok {
		return resp.Value{Type: resp.Integer, Number: 0}
	}
	if setCtx.AOF != nil {
		_ = setCtx.AOF.Append(resp.Value{Type: resp.Array, Items: []resp.Value{
			{Type: resp.BulkString, Text: "PEXPIREAT"},
			{Type: resp.BulkString, Text: key},
			{Type: resp.BulkString, Text: strconv.FormatInt(at.UnixMilli(), 10)},
		}})
	}
	return resp.Value{Type: resp.Integer, Number: 1}
}

func cmdSTTL(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'sttl'"}
	}
	key := args[0].Text
	return resp.Value{Type: resp.Integer, Number: setCtx.Set.TTL(key)}
}
