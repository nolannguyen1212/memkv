package core

import (
	"sync"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

type TransactionStatus uint8

const (
	TransactionIdle    TransactionStatus = iota
	TransactionQueuing TransactionStatus = iota
)

type QueuedCommand struct {
	Name string
	Args []protocol.Value
}

type TransactionState struct {
	Status  TransactionStatus
	Queue   []QueuedCommand
	Dirty   bool
	Watches map[string]uint64
}

func (transaction *TransactionState) Reset() {
	transaction.Status = TransactionIdle
	transaction.Queue = transaction.Queue[:0]
	transaction.Dirty = false
	transaction.Watches = nil
}

func (transaction *TransactionState) Enqueue(name string, args []protocol.Value) {
	transaction.Queue = append(transaction.Queue, QueuedCommand{Name: name, Args: args})
}

type watchEntry struct {
	connID  uint64
	isDirty *bool
}

type WatchRegistry struct {
	mutex    sync.Mutex
	watchers map[string][]watchEntry
}

var globalWatch = &WatchRegistry{
	watchers: make(map[string][]watchEntry),
}

func (registry *WatchRegistry) Watch(key string, connID uint64, isDirty *bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.watchers[key] = append(registry.watchers[key], watchEntry{connID: connID, isDirty: isDirty})
}

func (registry *WatchRegistry) Notify(key string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, entry := range registry.watchers[key] {
		*entry.isDirty = true
	}
}

func (registry *WatchRegistry) Unwatch(connID uint64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for key, entries := range registry.watchers {
		filtered := entries[:0]
		for _, entry := range entries {
			if entry.connID != connID {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			delete(registry.watchers, key)
		} else {
			registry.watchers[key] = filtered
		}
	}
}

func UnwatchConn(connID uint64) {
	globalWatch.Unwatch(connID)
}

func init() {
	Register("MULTI", handleMulti)
	Register("EXEC", handleExec)
	Register("DISCARD", handleDiscard)
	Register("WATCH", handleWatch)
	Register("UNWATCH", handleUnwatch)
}

func handleMulti(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR MULTI calls can not be nested")
	}
	connContext.Transaction.Status = TransactionQueuing
	return okReply()
}

func handleExec(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR EXEC without MULTI")
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
		return errorReply("ERR DISCARD without MULTI")
	}
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Reset()
	return okReply()
}

func handleWatch(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return wrongArgCountError("watch")
	}
	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR WATCH inside MULTI is not allowed")
	}
	if connContext.Transaction.Watches == nil {
		connContext.Transaction.Watches = make(map[string]uint64)
	}
	for _, arg := range args {
		key := arg.String
		connContext.Transaction.Watches[key] = connContext.Store.Dictionary.Version(key)
		globalWatch.Watch(key, connContext.ID, &connContext.Transaction.Dirty)
	}
	return okReply()
}

func handleUnwatch(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return okReply()
}
