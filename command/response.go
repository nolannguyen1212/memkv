package command

import "github.com/william1nguyen/valkeydb/protocol"

const (
	ResponseOK   = "OK"
	ResponsePONG = "PONG"
)

func OKResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: ResponseOK}
}

func PongResponse() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, String: ResponsePONG}
}

func ErrorResponse(message string) protocol.Value {
	return protocol.Value{Type: protocol.TypeError, String: message}
}

func IntegerResponse(number int64) protocol.Value {
	return protocol.Value{Type: protocol.TypeInteger, Number: number}
}

func StringResponse(text string) protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, String: text}
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

func WrongArgumentCountError(commandName string) protocol.Value {
	return ErrorResponse("ERR wrong number of arguments for '" + commandName + "'")
}

func NotIntegerError() protocol.Value {
	return ErrorResponse("ERR value is not an integer or out of range")
}

func SyntaxError() protocol.Value {
	return ErrorResponse("ERR syntax error")
}
