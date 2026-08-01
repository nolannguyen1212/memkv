package engine

import (
	"sync"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type TransactionStatus uint8

const (
	TransactionIdle    TransactionStatus = iota
	TransactionQueuing TransactionStatus = iota
)

type QueuedCommand struct {
	Name string
	Args []resp.Value
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

func (transaction *TransactionState) Enqueue(name string, args []resp.Value) {
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

func registerTransactionCommands(registry *Registry) {
	registry.Register("MULTI", handleMulti)
	registry.Register("EXEC", handleExec)
	registry.Register("DISCARD", handleDiscard)
	registry.Register("WATCH", handleWatch)
	registry.Register("UNWATCH", handleUnwatch)
}

func handleMulti(connContext *ConnContext, _ []resp.Value) resp.Value {
	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR MULTI calls can not be nested")
	}
	connContext.Transaction.Status = TransactionQueuing
	return okReply()
}

func handleExec(connContext *ConnContext, _ []resp.Value) resp.Value {
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
		return resp.Value{Type: resp.TypeArray, Array: nil}
	}
	for key, version := range connContext.Transaction.Watches {
		if connContext.Store.Keyspace.Version(key) != version {
			return resp.Value{Type: resp.TypeArray, Array: nil}
		}
	}

	connContext.Transaction.Status = TransactionIdle

	results := make([]resp.Value, len(connContext.Transaction.Queue))
	for i, cmd := range connContext.Transaction.Queue {
		results[i] = executeCommand(connContext, cmd.Name, cmd.Args)
	}
	return resp.Value{Type: resp.TypeArray, Array: results}
}

func handleDiscard(connContext *ConnContext, _ []resp.Value) resp.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR DISCARD without MULTI")
	}
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Reset()
	return okReply()
}

func handleWatch(connContext *ConnContext, args []resp.Value) resp.Value {
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
		connContext.Transaction.Watches[key] = connContext.Store.Keyspace.Version(key)
		globalWatch.Watch(key, connContext.ID)
	}
	return okReply()
}

func handleUnwatch(connContext *ConnContext, _ []resp.Value) resp.Value {
	globalWatch.Unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return okReply()
}
