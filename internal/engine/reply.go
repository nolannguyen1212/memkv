package engine

import (
	"github.com/william1nguyen/memkv/internal/mutation"
)

type ResultType uint8

const (
	ResultSimpleString ResultType = iota + 1
	ResultError
	ResultInteger
	ResultBulkString
	ResultArray
)

type Result struct {
	Type   ResultType
	String string
	Number int64
	Array  []Result
	IsNull bool
}

func okReply() Result {
	return Result{Type: ResultSimpleString, String: "OK"}
}

func pongReply() Result {
	return Result{Type: ResultSimpleString, String: "PONG"}
}

func errorReply(msg string) Result {
	return Result{Type: ResultError, String: msg}
}

func intReply(n int64) Result {
	return Result{Type: ResultInteger, Number: n}
}

func stringReply(s string) Result {
	return Result{Type: ResultBulkString, String: s}
}

func nullStringReply() Result {
	return Result{Type: ResultBulkString, IsNull: true}
}

func arrayReply(items []Result) Result {
	return Result{Type: ResultArray, Array: items}
}

func nullArrayReply() Result {
	return Result{Type: ResultArray, IsNull: true}
}

func emptyArrayReply() Result {
	return Result{Type: ResultArray, Array: []Result{}}
}

func wrongArgCountError(cmd string) Result {
	return errorReply("ERR wrong number of arguments for '" + cmd + "'")
}

func notIntegerError() Result {
	return errorReply("ERR value is not an integer or out of range")
}

func wrongTypeError() Result {
	return errorReply("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func persistenceError() Result {
	return errorReply("MISCONF Persistence failed; writes are disabled to prevent data loss")
}

func newMutation(items ...string) mutation.Command {
	return mutation.New(items[0], items[1:]...)
}
