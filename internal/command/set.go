package command

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func set(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'set'",
		}
	}

	key := args[0].Str
	val := args[1].Str
	ttl := time.Duration(0)

	if len(args) > 2 {
		if seconds, err := strconv.Atoi(args[2].Str); err == nil && seconds > 0 {
			ttl = time.Duration(seconds) * time.Second
		}
	}

	db.Set(key, val, ttl)

	return resp.Value{
		Type: resp.STRING,
		Str:  "OK",
	}
}
