package engine

import "github.com/william1nguyen/valkeydb/internal/resp"

func okReply() resp.Value {
	return resp.Value{Type: resp.TypeSimpleString, String: "OK"}
}

func pongReply() resp.Value {
	return resp.Value{Type: resp.TypeSimpleString, String: "PONG"}
}

func errorReply(msg string) resp.Value {
	return resp.Value{Type: resp.TypeError, String: msg}
}

func intReply(n int64) resp.Value {
	return resp.Value{Type: resp.TypeInteger, Number: n}
}

func stringReply(s string) resp.Value {
	return resp.Value{Type: resp.TypeBulkString, String: s}
}

func nullStringReply() resp.Value {
	return resp.Value{Type: resp.TypeBulkString, IsNull: true}
}

func arrayReply(items []resp.Value) resp.Value {
	return resp.Value{Type: resp.TypeArray, Array: items}
}

func nullArrayReply() resp.Value {
	return resp.Value{Type: resp.TypeArray, IsNull: true}
}

func emptyArrayReply() resp.Value {
	return resp.Value{Type: resp.TypeArray, Array: []resp.Value{}}
}

func wrongArgCountError(cmd string) resp.Value {
	return errorReply("ERR wrong number of arguments for '" + cmd + "'")
}

func notIntegerError() resp.Value {
	return errorReply("ERR value is not an integer or out of range")
}

func syntaxError() resp.Value {
	return errorReply("ERR syntax error")
}

func wrongTypeError() resp.Value {
	return errorReply("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func persistenceError() resp.Value {
	return errorReply("MISCONF Persistence failed; writes are disabled to prevent data loss")
}

func buildBulkArray(items ...string) resp.Value {
	arr := make([]resp.Value, len(items))
	for i, item := range items {
		arr[i] = resp.Value{Type: resp.TypeBulkString, String: item}
	}
	return resp.Value{Type: resp.TypeArray, Array: arr}
}
