package engine

import (
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Handler func(connContext *ConnContext, args []resp.Value) resp.Value

type WALAppender interface {
	Append(value resp.Value) error
}

type Replicator interface {
	ActiveReplicas() []string
	HandlePSYNC(connection net.Conn, replicaAddress, replicaID string, offset int64, getSnapshot func() []byte) error
	Info() string
	IsReplica() bool
	Promote()
	Propagate(data []byte)
	SetReplica(address, username, password string, dispatch func(string, []resp.Value))
}

type Context struct {
	Store             *store.Store
	WAL               WALAppender
	Replication       Replicator
	Registry          *Registry
	persistenceFailed atomic.Bool
}

func NewContext(store *store.Store, log WALAppender, replicationManager Replicator) *Context {
	return &Context{Store: store, WAL: log, Replication: replicationManager, Registry: NewRegistry()}
}

func (ctx *Context) OnKeyMutate(key string) {
	ctx.Store.Keyspace.BumpVersion(key)
	globalWatch.Notify(key)
}

func (ctx *Context) OnKeyWrite(key string) {
	ctx.OnKeyMutate(key)
	ctx.Store.Eviction.RecordInsert(key)
}

func (ctx *Context) OnKeyRead(key string) {
	ctx.Store.Eviction.RecordAccess(key)
}

func (ctx *Context) OnKeyDelete(key string) {
	ctx.OnKeyMutate(key)
	ctx.Store.Eviction.RecordDelete(key)
}

func (ctx *Context) AppendWAL(value resp.Value) error {
	if err := ctx.appendAndPropagate(value); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}
	for ctx.Store.Eviction.ShouldEvict() {
		key := ctx.Store.Eviction.EvictOne()
		if key == "" {
			break
		}
		globalWatch.Notify(key)
		if err := ctx.appendAndPropagate(buildBulkArray("DEL", key)); err != nil {
			ctx.persistenceFailed.Store(true)
			return err
		}
	}
	return nil
}

func (ctx *Context) appendAndPropagate(value resp.Value) error {
	if ctx.WAL != nil {
		if err := ctx.WAL.Append(value); err != nil {
			return err
		}
	}
	if ctx.Replication != nil {
		ctx.Replication.Propagate([]byte(resp.Encode(value)))
	}
	return nil
}

func (ctx *Context) GenerateRDB() []byte {
	var buffer bytes.Buffer

	for key, entry := range ctx.Store.Dictionary.Snapshot() {
		if entry.IsExpired() {
			continue
		}
		cmd := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: key},
			{Type: resp.TypeBulkString, String: entry.Value},
		}}
		buffer.WriteString(resp.Encode(cmd))
	}

	for key, entry := range ctx.Store.Set.Snapshot() {
		if entry.IsExpired() {
			continue
		}
		values := []resp.Value{
			{Type: resp.TypeBulkString, String: "SADD"},
			{Type: resp.TypeBulkString, String: key},
		}
		for member := range entry.Members {
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: member})
		}
		buffer.WriteString(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
	}

	for key, items := range ctx.Store.List.Snapshot() {
		if len(items) == 0 {
			continue
		}
		values := []resp.Value{
			{Type: resp.TypeBulkString, String: "RPUSH"},
			{Type: resp.TypeBulkString, String: key},
		}
		for _, item := range items {
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: item})
		}
		buffer.WriteString(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
	}

	for key, fields := range ctx.Store.Hash.Snapshot() {
		if len(fields) == 0 {
			continue
		}
		values := []resp.Value{
			{Type: resp.TypeBulkString, String: "HSET"},
			{Type: resp.TypeBulkString, String: key},
		}
		for field, value := range fields {
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: field})
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: value})
		}
		buffer.WriteString(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
	}

	for key, members := range ctx.Store.SortedSet.Snapshot() {
		if len(members) == 0 {
			continue
		}
		values := []resp.Value{
			{Type: resp.TypeBulkString, String: "ZADD"},
			{Type: resp.TypeBulkString, String: key},
		}
		for member, score := range members {
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: strconv.FormatFloat(score, 'g', -1, 64)})
			values = append(values, resp.Value{Type: resp.TypeBulkString, String: member})
		}
		buffer.WriteString(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
	}

	for key, metadata := range ctx.Store.Keyspace.Snapshot() {
		if metadata.ExpiredAt.IsZero() {
			continue
		}
		buffer.WriteString(resp.Encode(buildBulkArray(
			"PEXPIREAT", key, strconv.FormatInt(metadata.ExpiredAt.UnixMilli(), 10),
		)))
	}

	return buffer.Bytes()
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
	registerPubSubCommands(registry)
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
	"SEXPIRE":    true,
	"SDIFFSTORE": true, "SINTERSTORE": true, "SUNIONSTORE": true,
	"LPUSH": true, "RPUSH": true, "LPUSHX": true, "RPUSHX": true,
	"LPOP": true, "RPOP": true, "LSET": true, "LINSERT": true, "LREM": true, "LTRIM": true,
	"SORT": true,
	"HSET": true, "HMSET": true, "HDEL": true, "HSETNX": true,
	"HINCRBY": true, "HINCRBYFLOAT": true,
	"ZADD": true, "ZREM": true, "ZINCRBY": true, "ZPOPMIN": true, "ZPOPMAX": true,
	"FLUSHDB": true, "FLUSHALL": true,
}

func Execute(connContext *ConnContext, name string, args []resp.Value) resp.Value {
	log.Printf("[EXEC] %s %s", name, formatArgs(args))
	if connContext.Transaction.Status == TransactionQueuing && !txPassthrough[name] {
		connContext.Transaction.Enqueue(name, args)
		return resp.Value{Type: resp.TypeSimpleString, String: "QUEUED"}
	}
	if connContext.Connection != nil && connContext.Replication != nil && connContext.Replication.IsReplica() && writeCommands[name] {
		return resp.Value{Type: resp.TypeError, String: "READONLY You can't write against a read only replica."}
	}
	if writeCommands[name] && connContext.persistenceFailed.Load() {
		return persistenceError()
	}

	// EXEC owns the exclusive lock itself because it must keep that lock while
	// dispatching every queued command. All other commands share the same
	// boundary and therefore cannot run in the middle of an EXEC batch.
	if name == "EXEC" {
		return executeCommand(connContext, name, args)
	}
	if writeCommands[name] {
		connContext.Store.ExecMu.Lock()
		defer connContext.Store.ExecMu.Unlock()
		return executeCommand(connContext, name, args)
	}
	connContext.Store.ExecMu.RLock()
	defer connContext.Store.ExecMu.RUnlock()
	return executeCommand(connContext, name, args)
}

func executeCommand(connContext *ConnContext, name string, args []resp.Value) resp.Value {
	handler, exists := connContext.Registry.Lookup(name)
	if !exists {
		return errorReply("ERR unknown command '" + name + "'")
	}
	return handler(connContext, args)
}

func formatArgs(args []resp.Value) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = a.String
	}
	return strings.Join(parts, " ")
}
