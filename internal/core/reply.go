package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

func okReply() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
}

func pongReply() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: "PONG"}
}

func errorReply(msg string) protocol.Value {
	return protocol.Value{Type: protocol.TypeError, String: msg}
}

func intReply(n int64) protocol.Value {
	return protocol.Value{Type: protocol.TypeInteger, Number: n}
}

func stringReply(s string) protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, String: s}
}

func nullStringReply() protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, IsNull: true}
}

func arrayReply(items []protocol.Value) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: items}
}

func nullArrayReply() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, IsNull: true}
}

func emptyArrayReply() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{}}
}

func wrongArgCountError(cmd string) protocol.Value {
	return errorReply("ERR wrong number of arguments for '" + cmd + "'")
}

func notIntegerError() protocol.Value {
	return errorReply("ERR value is not an integer or out of range")
}

func syntaxError() protocol.Value {
	return errorReply("ERR syntax error")
}

func wrongTypeError() protocol.Value {
	return errorReply("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func persistenceError() protocol.Value {
	return errorReply("MISCONF Persistence failed; writes are disabled to prevent data loss")
}

func buildBulkArray(items ...string) protocol.Value {
	arr := make([]protocol.Value, len(items))
	for i, item := range items {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, String: item}
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}
