package engine

import (
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

type SnapshotCapture = func() (store.LogicalSnapshot, int64, error)

type Replicator interface {
	ActiveReplicas() []string
	HandlePSYNC(
		connection net.Conn,
		replicaAddress string,
		replicaID string,
		offset int64,
		captureSnapshot SnapshotCapture,
	) error
	Info() string
	IsReplica() bool
	Offset() int64
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
	events            chan engineEvent
	done              chan struct{}
	ready             chan struct{}
	running           atomic.Bool
	directMu          sync.Mutex
	deriveCapacity    bool
}

func NewContext(
	database *store.Store,
	log WALAppender,
	replicationManager Replicator,
	system SystemConfig,
) *Context {
	return newContext(database, log, replicationManager, system, true)
}

func newContext(
	database *store.Store,
	log WALAppender,
	replicationManager Replicator,
	system SystemConfig,
	deriveCapacity bool,
) *Context {
	ctx := &Context{
		Store:          database,
		WAL:            log,
		Replication:    replicationManager,
		Registry:       NewRegistry(),
		system:         system,
		watches:        newWatchRegistry(),
		metrics:        newMetrics(),
		events:         make(chan engineEvent, requestQueueCapacity),
		done:           make(chan struct{}),
		ready:          make(chan struct{}),
		deriveCapacity: deriveCapacity,
	}

	database.SetExpirationHandler(ctx.keyExpired)
	database.SetMutationHandler(ctx.watches.notify)
	return ctx
}

func (ctx *Context) keyExpired(key string) {
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

func (ctx *Context) ReplicationSnapshot() store.LogicalSnapshot {
	return ctx.Store.Snapshot()
}

func (ctx *Context) RestoreSnapshot(state store.LogicalSnapshot) error {
	if ctx.running.Load() {
		request := &restoreRequest{state: state, reply: make(chan error, 1)}

		select {
		case ctx.events <- *request:
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
	return ExecuteCommand(connContext, NewCommand(name, args))
}

func BootstrapExecute(connContext *ConnContext, name string, args []string) Result {
	connContext.directMu.Lock()
	defer connContext.directMu.Unlock()

	if connContext.running.Load() {
		return errorReply("ERR bootstrap execution is unavailable while engine is running")
	}

	connContext.Store.Maintain(connContext.Store.Now())
	return executeOwned(connContext, name, args)
}

func executeOwned(connContext *ConnContext, name string, args []string) Result {
	command, exists := connContext.Registry.command(name)

	if shouldQueue(connContext, command, exists) {
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

	if isReadOnlyWrite(connContext, command) {
		return Result{Type: ResultError, String: "READONLY You can't write against a read only replica."}
	}

	if command.write && connContext.persistenceFailed.Load() {
		return persistenceError()
	}

	return command.handler(connContext, args)
}

func shouldQueue(connContext *ConnContext, command registeredCommand, exists bool) bool {
	return connContext.Transaction.Status == TransactionQueuing &&
		(!exists || !command.transactionControl)
}

func isReadOnlyWrite(connContext *ConnContext, command registeredCommand) bool {
	return command.write &&
		connContext.Connection != nil &&
		connContext.Replication != nil &&
		connContext.Replication.IsReplica()
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
