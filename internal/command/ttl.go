package command

import "github.com/william1nguyen/valkeydb/internal/resp"

func ttl(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'ttl'",
		}
	}

	key := args[0].Str
	remaining := db.TTL(key)

	return resp.Value{
		Type: resp.INT,
		Int:  remaining,
	}
}
