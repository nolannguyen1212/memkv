package engine

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/mutation"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Handler func(connContext *ConnContext, args []string) Result

type WALAppender interface {
	Append(command mutation.Command) error
}

type WALBatchAppender interface {
	AppendBatch(batch mutation.Batch) error
}

type WALRewriter interface {
	RewriteAll(state store.LogicalSnapshot, path string) error
}

type WALOffsetter interface {
	Offset() (int64, error)
}

type Replicator interface {
	ActiveReplicas() []string
	HandlePSYNC(connection net.Conn, replicaAddress, replicaID string, offset int64, getSnapshot func() (store.LogicalSnapshot, error)) error
	Info() string
	IsReplica() bool
	Propagate(batch mutation.Batch)
}

type SystemConfig struct {
	Auth            string
	WALEnabled      bool
	SnapshotEnabled bool
}

type Context struct {
	Store             *store.Store
	WAL               WALAppender
	Replication       Replicator
	Registry          *Registry
	system            SystemConfig
	watches           *watchRegistry
	metrics           *metrics
	connectionCounter atomic.Uint64
	persistenceFailed atomic.Bool
	events            chan event
	done              chan struct{}
	ready             chan struct{}
	running           atomic.Bool
	directMu          sync.Mutex
}

func NewContext(database *store.Store, log WALAppender, replicationManager Replicator, system SystemConfig) *Context {
	ctx := &Context{
		Store:       database,
		WAL:         log,
		Replication: replicationManager,
		Registry:    NewRegistry(),
		system:      system,
		watches:     newWatchRegistry(),
		metrics:     newMetrics(),
		events:      make(chan event, requestQueueCapacity),
		done:        make(chan struct{}),
		ready:       make(chan struct{}),
	}
	database.SetExpirationHandler(ctx.keyExpired)
	return ctx
}

func (ctx *Context) keyExpired(key string) {
	ctx.watches.notify(key)
	ctx.propagate(mutation.Batch{mutation.New("DEL", key)})
}

func (ctx *Context) ConnectionOpened() {
	ctx.metrics.connectionOpened()
}

func (ctx *Context) ConnectionClosed() {
	ctx.metrics.connectionClosed()
}

func (ctx *Context) CommandProcessed() {
	ctx.metrics.totalCommands.Add(1)
}

func (ctx *Context) OnKeyMutate(key string) {
	ctx.Store.Keyspace.BumpVersion(key)
	ctx.watches.notify(key)
}

func (ctx *Context) OnKeyWrite(key string) {
	ctx.OnKeyMutate(key)
	ctx.Store.Eviction.RecordInsert(key)
}

func (ctx *Context) OnKeyRead(key string) {
	ctx.Store.Eviction.RecordAccess(key)
}

func (ctx *Context) Commit(command mutation.Command, apply func()) error {
	batch := mutation.Batch{command}
	if err := ctx.appendWAL(batch); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}
	apply()
	ctx.propagate(batch)
	return ctx.evictIfNeeded()
}

func (ctx *Context) CommitBatch(batch mutation.Batch, apply func()) error {
	if len(batch) == 0 {
		apply()
		return nil
	}
	if err := ctx.appendWAL(batch); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}
	apply()
	ctx.propagate(batch)
	return nil
}

func (ctx *Context) evictIfNeeded() error {
	for ctx.Store.Eviction.ShouldEvict() {
		key := ctx.Store.Eviction.NextEviction()
		if key == "" {
			break
		}
		batch := mutation.Batch{mutation.New("DEL", key)}
		if err := ctx.appendWAL(batch); err != nil {
			ctx.persistenceFailed.Store(true)
			return err
		}
		ctx.Store.DeleteKey(key)
		ctx.watches.notify(key)
		ctx.propagate(batch)
	}
	return nil
}

func (ctx *Context) appendWAL(batch mutation.Batch) error {
	if ctx.WAL == nil {
		return nil
	}
	if batchAppender, ok := ctx.WAL.(WALBatchAppender); ok {
		return batchAppender.AppendBatch(batch)
	}
	if len(batch) == 1 {
		return ctx.WAL.Append(batch[0])
	}
	return fmt.Errorf("WAL does not support atomic batches")
}

func (ctx *Context) propagate(batch mutation.Batch) {
	if ctx.Replication != nil {
		ctx.Replication.Propagate(batch)
	}
}

func (ctx *Context) ReplicationSnapshot() store.LogicalSnapshot {
	return ctx.Store.Snapshot()
}

func (ctx *Context) RestoreSnapshot(state store.LogicalSnapshot) error {
	if ctx.running.Load() {
		request := &restoreRequest{state: state, reply: make(chan error, 1)}
		select {
		case ctx.events <- event{restore: request}:
		case <-ctx.done:
			return ErrStopped
		}
		select {
		case err := <-request.reply:
			return err
		case <-ctx.done:
			return ErrStopped
		}
	}
	ctx.directMu.Lock()
	defer ctx.directMu.Unlock()
	return ctx.Store.Restore(state, ctx.Store.Now())
}

type Registry struct {
	commands map[string]registeredCommand
}

type registeredCommand struct {
	handler            Handler
	syntax             commandSpec
	write              bool
	transactionControl bool
}

type commandOption func(*registeredCommand)

func NewRegistry() *Registry {
	registry := &Registry{commands: make(map[string]registeredCommand)}
	registerStringCommands(registry)
	registerListCommands(registry)
	registerHashCommands(registry)
	registerSetCommands(registry)
	registerSortedSetCommands(registry)
	registerTransactionCommands(registry)
	registerSystemCommands(registry)
	registerReplicationCommands(registry)
	return registry
}

func (registry *Registry) Register(name string, handler Handler) {
	registry.register(name, handler)
}

func (registry *Registry) register(name string, handler Handler, options ...commandOption) {
	command := registeredCommand{handler: handler, syntax: commandSpec{maxArgs: -1}}
	for _, option := range options {
		option(&command)
	}
	registry.commands[strings.ToUpper(name)] = command
}

func (registry *Registry) Lookup(name string) (Handler, bool) {
	command, ok := registry.commands[strings.ToUpper(name)]
	return command.handler, ok
}

func (registry *Registry) command(name string) (registeredCommand, bool) {
	command, ok := registry.commands[strings.ToUpper(name)]
	return command, ok
}

func Execute(connContext *ConnContext, name string, args []string) Result {
	return execute(connContext, NewCommand(name, args))
}

func execute(connContext *ConnContext, command Command) Result {
	if connContext.running.Load() {
		result, err := connContext.SubmitCommand(connContext, command)
		if err != nil {
			return errorReply("ERR engine stopped")
		}
		return result
	}
	connContext.directMu.Lock()
	defer connContext.directMu.Unlock()
	connContext.Store.Maintain(connContext.Store.Now())
	return executeOwned(connContext, command.Name, command.Args)
}

func executeOwned(connContext *ConnContext, name string, args []string) Result {
	command, exists := connContext.Registry.command(name)
	if connContext.Transaction.Status == TransactionQueuing && (!exists || !command.transactionControl) {
		if !exists {
			connContext.Transaction.Dirty = true
			return errorReply("ERR unknown command '" + name + "'")
		}
		if validationError := validateRegisteredCommand(name, command, args); validationError != nil {
			connContext.Transaction.Dirty = true
			return *validationError
		}
		connContext.Transaction.Enqueue(name, args)
		return Result{Type: ResultSimpleString, String: "QUEUED"}
	}
	if !exists {
		return errorReply("ERR unknown command '" + name + "'")
	}
	if validationError := validateRegisteredCommand(name, command, args); validationError != nil {
		return *validationError
	}
	if connContext.Connection != nil && connContext.Replication != nil && connContext.Replication.IsReplica() && command.write {
		return Result{Type: ResultError, String: "READONLY You can't write against a read only replica."}
	}
	if command.write && connContext.persistenceFailed.Load() {
		return persistenceError()
	}

	return command.handler(connContext, args)
}

func executeCommand(connContext *ConnContext, name string, args []string) Result {
	command, exists := connContext.Registry.command(name)
	if !exists {
		return errorReply("ERR unknown command '" + name + "'")
	}
	if validationError := validateRegisteredCommand(name, command, args); validationError != nil {
		return *validationError
	}
	return command.handler(connContext, args)
}

func validateRegisteredCommand(name string, command registeredCommand, args []string) *Result {
	if !command.syntax.validArgumentCount(args) {
		result := wrongArgCountError(strings.ToLower(name))
		return &result
	}
	if !command.syntax.valid(args) {
		result := errorReply("ERR syntax error")
		return &result
	}
	return nil
}
