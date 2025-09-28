package command

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func pexpireat(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'pexpireat'",
		}
	}

	key := args[0].Str
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)

	if err != nil {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR invalid expire time",
		}
	}

	at := time.UnixMilli(ms)
	ok := db.ExpireAt(key, at)

	if !ok {
		return resp.Value{
			Type: resp.INT,
			Int:  0,
		}
	}

	return resp.Value{
		Type: resp.INT,
		Int:  1,
	}
}
