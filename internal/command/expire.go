package command

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func expire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'expire'",
		}
	}

	key := args[0].Str
	seconds, err := strconv.Atoi(args[1].Str)

	if err != nil {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR value is not an integer or out of range",
		}
	}

	ok := db.Expire(key, time.Duration(seconds)*time.Second)
	if ok {
		return resp.Value{
			Type: resp.INT,
			Int:  1,
		}
	}

	return resp.Value{
		Type: resp.INT,
		Int:  0,
	}
}
