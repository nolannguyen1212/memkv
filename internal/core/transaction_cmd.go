package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

func init() {
	Register("MULTI", handleMulti)
	Register("EXEC", handleExec)
	Register("DISCARD", handleDiscard)
	Register("WATCH", handleWatch)
	Register("UNWATCH", handleUnwatch)
}

func handleMulti(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	if connContext.Transaction.Status == TransactionQueuing {
		return ErrorResponse("ERR MULTI calls can not be nested")
	}
	connContext.Transaction.Status = TransactionQueuing
	return OKResponse()
}

func handleExec(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return ErrorResponse("ERR EXEC without MULTI")
	}

	defer func() {
		globalWatch.Unwatch(connContext.ID)
		connContext.Transaction.Reset()
	}()

	if connContext.Transaction.Dirty {
		return protocol.Value{Type: protocol.TypeArray, Array: nil}
	}

	connContext.Store.ExecMu.Lock()
	defer connContext.Store.ExecMu.Unlock()

	connContext.Transaction.Status = TransactionIdle

	results := make([]protocol.Value, len(connContext.Transaction.Queue))
	for i, cmd := range connContext.Transaction.Queue {
		results[i] = Execute(connContext, cmd.Name, cmd.Args)
	}

	return protocol.Value{Type: protocol.TypeArray, Array: results}
}

func handleDiscard(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return ErrorResponse("ERR DISCARD without MULTI")
	}
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Reset()
	return OKResponse()
}

func handleWatch(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return WrongArgCountError("watch")
	}
	if connContext.Transaction.Status == TransactionQueuing {
		return ErrorResponse("ERR WATCH inside MULTI is not allowed")
	}
	if connContext.Transaction.Watches == nil {
		connContext.Transaction.Watches = make(map[string]uint64)
	}
	for _, arg := range args {
		key := arg.String
		connContext.Transaction.Watches[key] = connContext.Store.Dictionary.Version(key)
		globalWatch.Watch(key, connContext.ID, &connContext.Transaction.Dirty)
	}
	return OKResponse()
}

func handleUnwatch(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return OKResponse()
}
