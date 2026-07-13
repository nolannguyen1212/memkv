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
	connID uint64
}

type WatchRegistry struct {
	mutex    sync.Mutex
	watchers map[string][]watchEntry
	dirty    map[uint64]bool
}

var globalWatch = &WatchRegistry{
	watchers: make(map[string][]watchEntry),
	dirty:    make(map[uint64]bool),
}

func (registry *WatchRegistry) Watch(key string, connID uint64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, entry := range registry.watchers[key] {
		if entry.connID == connID {
			return
		}
	}
	registry.watchers[key] = append(registry.watchers[key], watchEntry{connID: connID})
}

func (registry *WatchRegistry) Notify(key string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for _, entry := range registry.watchers[key] {
		registry.dirty[entry.connID] = true
	}
}

func (registry *WatchRegistry) IsDirty(connID uint64) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.dirty[connID]
}

func (registry *WatchRegistry) Unwatch(connID uint64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.dirty, connID)
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

	connContext.Store.ExecMu.Lock()
	defer connContext.Store.ExecMu.Unlock()

	// Check WATCH after acquiring the transaction boundary. Any ordinary
	// command that started first has now completed its mutation notification;
	// commands that start later cannot run until this EXEC finishes.
	if connContext.Transaction.Dirty || globalWatch.IsDirty(connContext.ID) {
		return protocol.Value{Type: protocol.TypeArray, Array: nil}
	}

	connContext.Transaction.Status = TransactionIdle

	results := make([]protocol.Value, len(connContext.Transaction.Queue))
	for i, cmd := range connContext.Transaction.Queue {
		results[i] = executeCommand(connContext, cmd.Name, cmd.Args)
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
		globalWatch.Watch(key, connContext.ID)
	}
	return okReply()
}

func handleUnwatch(connContext *ConnContext, _ []protocol.Value) protocol.Value {
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return okReply()
}
