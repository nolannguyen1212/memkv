package command

import (
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

var globalStore = store.New()

func init() {
	Register("SET", set)
}

func set(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'set'",
		}
	}
	key := args[0].Str
	val := args[1].Str
	globalStore.Set(key, val)
	return resp.Value{
		Type: resp.STRING,
		Str:  "OK",
	}
}
