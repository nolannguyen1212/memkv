package engine

import (
	"fmt"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type TransactionStatus uint8

const (
	TransactionIdle    TransactionStatus = iota
	TransactionQueuing TransactionStatus = iota
)

type QueuedCommand = Command

type transactionWAL struct {
	values []resp.Value
}

func (log *transactionWAL) Append(value resp.Value) error {
	log.values = append(log.values, value)
	return nil
}

func (ctx *Context) Replay(commands []QueuedCommand) error {
	ctx.directMu.Lock()
	defer ctx.directMu.Unlock()

	now := ctx.Store.Now()
	stagedStore, err := ctx.Store.Clone(now)
	if err != nil {
		return err
	}
	stagedBase := NewContext(stagedStore, &transactionWAL{}, nil, ctx.system)
	stagedBase.Registry = ctx.Registry
	stagedConnection := NewConnContext(stagedBase, nil)
	for _, command := range commands {
		result := executeCommand(stagedConnection, command.Name, command.Args)
		if result.Type == resp.TypeError {
			return fmt.Errorf("execute %s: %s", command.Name, result.String)
		}
	}
	return ctx.Store.Restore(stagedStore.Snapshot(), now)
}

func (ctx *Context) applyBatchOwned(commands []QueuedCommand) error {
	now := ctx.Store.Now()
	stagedStore, err := ctx.Store.Clone(now)
	if err != nil {
		return err
	}
	stagedWAL := &transactionWAL{}
	stagedBase := NewContext(stagedStore, stagedWAL, nil, ctx.system)
	stagedBase.Registry = ctx.Registry
	stagedConnection := NewConnContext(stagedBase, nil)
	for _, command := range commands {
		result := executeCommand(stagedConnection, command.Name, command.Args)
		if result.Type == resp.TypeError {
			return fmt.Errorf("execute %s: %s", command.Name, result.String)
		}
	}
	before := ctx.Store.Snapshot()
	after := stagedStore.Snapshot()
	return ctx.CommitBatch(stagedWAL.values, func() {
		if err := ctx.Store.Restore(after, now); err != nil {
			panic(err)
		}
		notifyChangedVersions(ctx.watches, before.Versions, after.Versions)
	})
}

func notifyChangedVersions(watches *watchRegistry, before, after map[string]uint64) {
	for key, version := range after {
		if before[key] != version {
			watches.notify(key)
		}
	}
	for key := range before {
		if _, exists := after[key]; !exists {
			watches.notify(key)
		}
	}
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

func (transaction *TransactionState) Enqueue(name string, args []string) {
	transaction.Queue = append(transaction.Queue, Command{Name: name, Args: append([]string(nil), args...)})
}

type watchEntry struct {
	connID uint64
}

type watchRegistry struct {
	watchers map[string][]watchEntry
	dirty    map[uint64]bool
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{
		watchers: make(map[string][]watchEntry),
		dirty:    make(map[uint64]bool),
	}
}

func (registry *watchRegistry) watch(key string, connID uint64) {
	for _, entry := range registry.watchers[key] {
		if entry.connID == connID {
			return
		}
	}
	registry.watchers[key] = append(registry.watchers[key], watchEntry{connID: connID})
}

func (registry *watchRegistry) notify(key string) {
	for _, entry := range registry.watchers[key] {
		registry.dirty[entry.connID] = true
	}
}

func (registry *watchRegistry) isDirty(connID uint64) bool {
	return registry.dirty[connID]
}

func (registry *watchRegistry) unwatch(connID uint64) {
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

func registerTransactionCommands(registry *Registry) {
	registry.Register("MULTI", handleMulti)
	registry.Register("EXEC", handleExec)
	registry.Register("DISCARD", handleDiscard)
	registry.Register("WATCH", handleWatch)
	registry.Register("UNWATCH", handleUnwatch)
}

func handleMulti(connContext *ConnContext, _ []string) resp.Value {
	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR MULTI calls can not be nested")
	}
	connContext.Transaction.Status = TransactionQueuing
	return okReply()
}

func handleExec(connContext *ConnContext, _ []string) resp.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR EXEC without MULTI")
	}

	defer func() {
		connContext.watches.unwatch(connContext.ID)
		connContext.Transaction.Reset()
	}()

	if connContext.Transaction.Dirty || connContext.watches.isDirty(connContext.ID) {
		return resp.Value{Type: resp.TypeArray, Array: nil}
	}
	for key, version := range connContext.Transaction.Watches {
		if connContext.Store.Keyspace.Version(key) != version {
			return resp.Value{Type: resp.TypeArray, Array: nil}
		}
	}

	connContext.Transaction.Status = TransactionIdle
	now := connContext.Store.Now()
	stagedStore, err := connContext.Store.Clone(now)
	if err != nil {
		return errorReply("ERR transaction planning failed")
	}
	stagedWAL := &transactionWAL{}
	stagedBase := NewContext(stagedStore, stagedWAL, nil, connContext.system)
	stagedBase.Registry = connContext.Registry
	stagedConnection := NewConnContext(stagedBase, nil)

	results := make([]resp.Value, len(connContext.Transaction.Queue))
	for index, command := range connContext.Transaction.Queue {
		results[index] = executeCommand(stagedConnection, command.Name, command.Args)
	}
	if len(stagedWAL.values) == 0 {
		return resp.Value{Type: resp.TypeArray, Array: results}
	}
	before := connContext.Store.Snapshot()
	after := stagedStore.Snapshot()
	if err := connContext.CommitBatch(stagedWAL.values, func() {
		if err := connContext.Store.Restore(after, now); err != nil {
			panic(err)
		}
		for key, version := range after.Versions {
			if before.Versions[key] != version {
				connContext.watches.notify(key)
			}
		}
		for key := range before.Versions {
			if _, exists := after.Versions[key]; !exists {
				connContext.watches.notify(key)
			}
		}
	}); err != nil {
		return persistenceError()
	}
	return resp.Value{Type: resp.TypeArray, Array: results}
}

func handleDiscard(connContext *ConnContext, _ []string) resp.Value {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR DISCARD without MULTI")
	}
	connContext.watches.unwatch(connContext.ID)
	connContext.Transaction.Reset()
	return okReply()
}

func handleWatch(connContext *ConnContext, args []string) resp.Value {
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
		key := arg
		connContext.Transaction.Watches[key] = connContext.Store.Keyspace.Version(key)
		connContext.watches.watch(key, connContext.ID)
	}
	return okReply()
}

func handleUnwatch(connContext *ConnContext, _ []string) resp.Value {
	connContext.watches.unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return okReply()
}
