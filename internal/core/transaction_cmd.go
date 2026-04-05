package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

func init() {
	Register("MULTI", handleMulti)
	Register("EXEC", handleExec)
	Register("DISCARD", handleDiscard)
	Register("WATCH", handleWatch)
	Register("UNWATCH", handleUnwatch)
}

func handleMulti(cctx *ConnContext, _ []protocol.Value) protocol.Value {
	if cctx.TX.Status == TxQueueing {
		return ErrorResponse("ERR MULTI calls can not be nested")
	}
	cctx.TX.Status = TxQueueing
	return OKResponse()
}

func handleExec(cctx *ConnContext, _ []protocol.Value) protocol.Value {
	if cctx.TX.Status != TxQueueing {
		return ErrorResponse("ERR EXEC without MULTI")
	}

	defer func() {
		globalWatch.Unwatch(cctx.ID)
		cctx.TX.Reset()
	}()

	if cctx.TX.Dirty {
		return protocol.Value{Type: protocol.TypeArray, Array: nil}
	}

	cctx.Store.ExecMu.Lock()
	defer cctx.Store.ExecMu.Unlock()

	cctx.TX.Status = TxIdle

	results := make([]protocol.Value, len(cctx.TX.Queue))
	for i, cmd := range cctx.TX.Queue {
		results[i] = Execute(cctx, cmd.Name, cmd.Args)
	}

	return protocol.Value{Type: protocol.TypeArray, Array: results}
}

func handleDiscard(cctx *ConnContext, _ []protocol.Value) protocol.Value {
	if cctx.TX.Status != TxQueueing {
		return ErrorResponse("ERR DISCARD without MULTI")
	}
	globalWatch.Unwatch(cctx.ID)
	cctx.TX.Reset()
	return OKResponse()
}

func handleWatch(cctx *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return WrongArgCountError("watch")
	}
	if cctx.TX.Status == TxQueueing {
		return ErrorResponse("ERR WATCH inside MULTI is not allowed")
	}
	if cctx.TX.Watches == nil {
		cctx.TX.Watches = make(map[string]uint64)
	}
	for _, a := range args {
		key := a.String
		cctx.TX.Watches[key] = cctx.Store.Dictionary.Version(key)
		globalWatch.Watch(key, cctx.ID, &cctx.TX.Dirty)
	}
	return OKResponse()
}

func handleUnwatch(cctx *ConnContext, _ []protocol.Value) protocol.Value {
	globalWatch.Unwatch(cctx.ID)
	cctx.TX.Dirty = false
	cctx.TX.Watches = nil
	return OKResponse()
}
