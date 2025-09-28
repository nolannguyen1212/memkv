package command

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/aof"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Handler func(args []resp.Value) resp.Value

var (
	registry   = map[string]Handler{}
	db         store.Store
	aofHandler *aof.AOF
)

func Init(s store.Store, h *aof.AOF) {
	db = s
	Register("SET", set)
	Register("GET", get)
	Register("DEL", del)
	Register("EXPIRE", expire)
	Register("TTL", ttl)
	Register("PING", ping)
	Register("PEXPIREAT", pexpireat)

	aofHandler = h
}

func Register(name string, h Handler) {
	registry[strings.ToUpper(name)] = h
}

func Lookup(name string) (Handler, bool) {
	h, ok := registry[strings.ToUpper(name)]
	return h, ok
}

func Replay(cmd string, args []resp.Value) {
	h, ok := Lookup(cmd)
	if !ok {
		return
	}
	h(args)
}
