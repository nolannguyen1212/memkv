package command

import "github.com/william1nguyen/valkeydb/internal/resp"

func get(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR wrong number of arguments for 'get'",
		}
	}

	key := args[0].Str
	val, ok := db.Get(key)

	if !ok {
		return resp.Value{
			Type:  resp.BULKSTRING,
			IsNil: true,
		}
	}

	return resp.Value{
		Type: resp.BULKSTRING,
		Str:  val,
	}
}
