package command

import "github.com/william1nguyen/valkeydb/internal/resp"

func init() {
	Register("DEL", del)
}

func del(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'del'",
		}
	}

	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}

	n := db.Delete(keys...)

	return resp.Value{
		Type: resp.INT,
		Int:  int64(n),
	}
}
