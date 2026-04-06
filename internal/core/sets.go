package core

import (
	"strconv"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	Register("SADD", handleSadd)
	Register("SREM", handleSrem)
	Register("SMEMBERS", handleSmembers)
	Register("SISMEMBER", handleSismember)
	Register("SCARD", handleScard)
	Register("SEXPIRE", handleSexpire)
	Register("STTL", handleSttl)
}

func handleSadd(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("sadd")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := connContext.Store.Set.Add(key, members...)

	connContext.OnKeyWrite(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"SADD", key}, members...)...))

	return intReply(int64(count))
}

func handleSrem(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgCountError("srem")
	}

	key := args[0].String
	members := extractStrings(args[1:])
	count := connContext.Store.Set.Remove(key, members...)

	connContext.OnKeyMutate(key)
	connContext.AppendAOF(buildBulkArray(append([]string{"SREM", key}, members...)...))

	return intReply(int64(count))
}

func handleSmembers(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("smembers")
	}

	connContext.OnKeyRead(args[0].String)
	members, exists := connContext.Store.Set.Members(args[0].String)
	if !exists {
		return nullArrayReply()
	}

	items := make([]protocol.Value, len(members))
	for i, member := range members {
		items[i] = stringReply(member)
	}
	return arrayReply(items)
}

func handleSismember(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("sismember")
	}

	connContext.OnKeyRead(args[0].String)
	if connContext.Store.Set.IsMember(args[0].String, args[1].String) {
		return intReply(1)
	}
	return intReply(0)
}

func handleScard(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("scard")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(int64(connContext.Store.Set.Cardinality(args[0].String)))
}

func handleSexpire(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("sexpire")
	}

	key := args[0].String
	seconds, err := strconv.Atoi(args[1].String)
	if err != nil {
		return notIntegerError()
	}

	if !connContext.Store.Set.Expire(key, time.Duration(seconds)*time.Second) {
		return intReply(0)
	}
	return intReply(1)
}

func handleSttl(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("sttl")
	}

	connContext.OnKeyRead(args[0].String)
	return intReply(connContext.Store.Set.TTL(args[0].String))
}

// extractStrings converts a slice of protocol.Value to a slice of strings.
func extractStrings(args []protocol.Value) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = arg.String
	}
	return result
}
