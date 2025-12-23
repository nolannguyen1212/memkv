package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

func OKResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
}

func PongResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: "PONG"}
}

func ErrorResponse(msg string) protocol.Value {
	return protocol.Value{Type: protocol.TypeError, String: msg}
}

func IntegerResponse(n int64) protocol.Value {
	return protocol.Value{Type: protocol.TypeInteger, Number: n}
}

func StringResponse(s string) protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, String: s}
}

func NullStringResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, IsNull: true}
}

func ArrayResponse(items []protocol.Value) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: items}
}

func NullArrayResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, IsNull: true}
}

func EmptyArrayResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{}}
}

func WrongArgCountError(cmd string) protocol.Value {
	return ErrorResponse("ERR wrong number of arguments for '" + cmd + "'")
}

func NotIntegerError() protocol.Value {
	return ErrorResponse("ERR value is not an integer or out of range")
}

func SyntaxError() protocol.Value {
	return ErrorResponse("ERR syntax error")
}

func BuildBulkArray(items ...string) protocol.Value {
	arr := make([]protocol.Value, len(items))
	for i, item := range items {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, String: item}
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}
