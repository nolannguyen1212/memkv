package command

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type Handler func(args []resp.Value) resp.Value

type DB struct {
	Dict *datastructure.Dict
	AOF  *persistence.AOF
}

var (
	registry = map[string]Handler{}
)

func Init(db *DB) {
	registry = map[string]Handler{}

	SetDictContext(&DictContext{Dict: db.Dict, AOF: db.AOF})
	InitDictCommands()
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
