package command

import "github.com/william1nguyen/valkeydb/internal/resp"

func init() {
	Register("PING", ping)
}

func ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{
			Type: resp.STRING,
			Str:  "PONG",
		}
	}
	return resp.Value{
		Type: resp.BULKSTRING,
		Str:  args[0].Str,
	}
}
