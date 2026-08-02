package engine

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Handler func(connContext *ConnContext, args []string) resp.Value

type WALAppender interface {
	Append(value resp.Value) error
}

type WALBatchAppender interface {
	AppendBatch(values []resp.Value) error
}

type WALRewriter interface {
	RewriteAll(state store.LogicalSnapshot, path string) error
}

type WALOffsetter interface {
	Offset() (int64, error)
}

type Replicator interface {
	ActiveReplicas() []string
	HandlePSYNC(connection net.Conn, replicaAddress, replicaID string, offset int64, getSnapshot func() store.LogicalSnapshot) error
	Info() string
	IsReplica() bool
	Propagate(data []byte)
	PropagateBatch(data []byte)
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
	ctx.propagate(buildBulkArray("DEL", key))
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

func (ctx *Context) Commit(value resp.Value, apply func()) error {
	if err := ctx.appendWAL(value); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}
	apply()
	ctx.propagate(value)
	return ctx.evictIfNeeded()
}

func (ctx *Context) CommitBatch(values []resp.Value, apply func()) error {
	if len(values) == 0 {
		apply()
		return nil
	}
	if err := ctx.appendWALBatch(values); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}
	apply()
	if ctx.Replication != nil {
		ctx.Replication.PropagateBatch([]byte(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values})))
	}
	return nil
}

func (ctx *Context) evictIfNeeded() error {
	for ctx.Store.Eviction.ShouldEvict() {
		key := ctx.Store.Eviction.NextEviction()
		if key == "" {
			break
		}
		ctx.watches.notify(key)
		value := buildBulkArray("DEL", key)
		if err := ctx.appendWAL(value); err != nil {
			ctx.persistenceFailed.Store(true)
			return err
		}
		ctx.Store.DeleteKey(key)
		ctx.watches.notify(key)
		ctx.propagate(value)
	}
	return nil
}

func (ctx *Context) appendWAL(value resp.Value) error {
	if ctx.WAL != nil {
		if err := ctx.WAL.Append(value); err != nil {
			return err
		}
	}
	return nil
}

func (ctx *Context) appendWALBatch(values []resp.Value) error {
	if ctx.WAL == nil {
		return nil
	}
	if batchAppender, ok := ctx.WAL.(WALBatchAppender); ok {
		return batchAppender.AppendBatch(values)
	}
	if len(values) == 1 {
		return ctx.WAL.Append(values[0])
	}
	return fmt.Errorf("WAL does not support atomic batches")
}

func (ctx *Context) propagate(value resp.Value) {
	if ctx.Replication != nil {
		ctx.Replication.Propagate([]byte(resp.Encode(value)))
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
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	registry := &Registry{handlers: make(map[string]Handler)}
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
	registry.handlers[strings.ToUpper(name)] = handler
}

func (registry *Registry) Lookup(name string) (Handler, bool) {
	handler, ok := registry.handlers[strings.ToUpper(name)]
	return handler, ok
}

var txPassthrough = map[string]bool{
	"MULTI":    true,
	"EXEC":     true,
	"DISCARD":  true,
	"WATCH":    true,
	"UNWATCH":  true,
	"QUIT":     true,
	"PSYNC":    true,
	"REPLCONF": true,
	"RAFT":     true,
}

var writeCommands = map[string]bool{
	"SET": true, "MSET": true, "SETEX": true, "SETNX": true, "PSETEX": true,
	"GETSET": true, "GETDEL": true, "GETEX": true,
	"DEL": true, "UNLINK": true, "RENAME": true, "RENAMENX": true,
	"INCR": true, "INCRBY": true, "INCRBYFLOAT": true, "DECR": true, "DECRBY": true,
	"APPEND": true, "SETRANGE": true,
	"EXPIRE": true, "PEXPIRE": true, "EXPIREAT": true, "PEXPIREAT": true, "PERSIST": true,
	"SADD": true, "SREM": true, "SMOVE": true, "SPOP": true,
	"SDIFFSTORE": true, "SINTERSTORE": true, "SUNIONSTORE": true,
	"LPUSH": true, "RPUSH": true, "LPUSHX": true, "RPUSHX": true,
	"LPOP": true, "RPOP": true, "LSET": true, "LINSERT": true, "LREM": true, "LTRIM": true,
	"HSET": true, "HMSET": true, "HDEL": true, "HSETNX": true,
	"HINCRBY": true, "HINCRBYFLOAT": true,
	"ZADD": true, "ZREM": true, "ZINCRBY": true, "ZPOPMIN": true, "ZPOPMAX": true,
	"FLUSHDB": true, "FLUSHALL": true,
}

func Execute(connContext *ConnContext, name string, args []resp.Value) resp.Value {
	return execute(connContext, NewCommand(name, args))
}

func execute(connContext *ConnContext, command Command) resp.Value {
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

func executeOwned(connContext *ConnContext, name string, args []string) resp.Value {
	log.Printf("[EXEC] %s %s", name, formatArgs(args))
	if connContext.Transaction.Status == TransactionQueuing && !txPassthrough[name] {
		if _, exists := connContext.Registry.Lookup(name); !exists || !validateCommand(name, args) {
			connContext.Transaction.Dirty = true
			return errorReply("ERR invalid command syntax")
		}
		connContext.Transaction.Enqueue(name, args)
		return resp.Value{Type: resp.TypeSimpleString, String: "QUEUED"}
	}
	if connContext.Connection != nil && connContext.Replication != nil && connContext.Replication.IsReplica() && writeCommands[name] {
		return resp.Value{Type: resp.TypeError, String: "READONLY You can't write against a read only replica."}
	}
	if writeCommands[name] && connContext.persistenceFailed.Load() {
		return persistenceError()
	}

	return executeCommand(connContext, name, args)
}

func executeCommand(connContext *ConnContext, name string, args []string) resp.Value {
	handler, exists := connContext.Registry.Lookup(name)
	if !exists {
		return errorReply("ERR unknown command '" + name + "'")
	}
	return handler(connContext, args)
}

func formatArgs(args []string) string {
	parts := make([]string, len(args))
	copy(parts, args)
	return strings.Join(parts, " ")
}
