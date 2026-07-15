package core

import (
	"bytes"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/protocol"
	"github.com/william1nguyen/valkeydb/internal/replication"
)

type Handler func(connContext *ConnContext, args []protocol.Value) protocol.Value

type AOFAppender interface {
	Append(value protocol.Value) error
}

type Context struct {
	Store             *Store
	AOF               AOFAppender
	Replication       *replication.Manager
	persistenceFailed atomic.Bool
}

func NewContext(store *Store, aof AOFAppender, replicationManager *replication.Manager) *Context {
	return &Context{Store: store, AOF: aof, Replication: replicationManager}
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

func (ctx *Context) AppendAOF(value protocol.Value) error {
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

func (ctx *Context) appendAndPropagate(value protocol.Value) error {
	if ctx.AOF != nil {
		if err := ctx.AOF.Append(value); err != nil {
			return err
		}
	}
	if ctx.Replication != nil {
		ctx.Replication.Propagate([]byte(protocol.Encode(value)))
	}
	return nil
}

func (ctx *Context) GenerateRDB() []byte {
	var buffer bytes.Buffer

	for key, entry := range ctx.Store.Dictionary.Snapshot() {
		if entry.IsExpired() {
			continue
		}
		cmd := protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
			{Type: protocol.TypeBulkString, String: "SET"},
			{Type: protocol.TypeBulkString, String: key},
			{Type: protocol.TypeBulkString, String: entry.Value},
		}}
		buffer.WriteString(protocol.Encode(cmd))
	}

	for key, entry := range ctx.Store.Set.Snapshot() {
		if entry.IsExpired() {
			continue
		}
		values := []protocol.Value{
			{Type: protocol.TypeBulkString, String: "SADD"},
			{Type: protocol.TypeBulkString, String: key},
		}
		for member := range entry.Members {
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: member})
		}
		buffer.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: values}))
	}

	for key, items := range ctx.Store.List.Snapshot() {
		if len(items) == 0 {
			continue
		}
		values := []protocol.Value{
			{Type: protocol.TypeBulkString, String: "RPUSH"},
			{Type: protocol.TypeBulkString, String: key},
		}
		for _, item := range items {
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: item})
		}
		buffer.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: values}))
	}

	for key, fields := range ctx.Store.Hash.Snapshot() {
		if len(fields) == 0 {
			continue
		}
		values := []protocol.Value{
			{Type: protocol.TypeBulkString, String: "HSET"},
			{Type: protocol.TypeBulkString, String: key},
		}
		for field, value := range fields {
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: field})
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: value})
		}
		buffer.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: values}))
	}

	for key, members := range ctx.Store.SortedSet.Snapshot() {
		if len(members) == 0 {
			continue
		}
		values := []protocol.Value{
			{Type: protocol.TypeBulkString, String: "ZADD"},
			{Type: protocol.TypeBulkString, String: key},
		}
		for member, score := range members {
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: strconv.FormatFloat(score, 'g', -1, 64)})
			values = append(values, protocol.Value{Type: protocol.TypeBulkString, String: member})
		}
		buffer.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: values}))
	}

	for key, metadata := range ctx.Store.Keyspace.Snapshot() {
		if metadata.ExpiredAt.IsZero() {
			continue
		}
		buffer.WriteString(protocol.Encode(buildBulkArray(
			"PEXPIREAT", key, strconv.FormatInt(metadata.ExpiredAt.UnixMilli(), 10),
		)))
	}

	return buffer.Bytes()
}

var (
	registryMutex sync.RWMutex
	handlers      = make(map[string]Handler)
)

func Register(name string, handler Handler) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	handlers[strings.ToUpper(name)] = handler
}

func Lookup(name string) (Handler, bool) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	handler, ok := handlers[strings.ToUpper(name)]
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

func Execute(connContext *ConnContext, name string, args []protocol.Value) protocol.Value {
	log.Printf("[EXEC] %s %s", name, formatArgs(args))
	if connContext.Transaction.Status == TransactionQueuing && !txPassthrough[name] {
		connContext.Transaction.Enqueue(name, args)
		return protocol.Value{Type: protocol.TypeSimpleString, String: "QUEUED"}
	}
	if connContext.Connection != nil && connContext.Replication != nil && connContext.Replication.IsReplica() && writeCommands[name] {
		return protocol.Value{Type: protocol.TypeError, String: "READONLY You can't write against a read only replica."}
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

func executeCommand(connContext *ConnContext, name string, args []protocol.Value) protocol.Value {
	handler, exists := Lookup(name)
	if !exists {
		return errorReply("ERR unknown command '" + name + "'")
	}
	return handler(connContext, args)
}

func formatArgs(args []protocol.Value) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = a.String
	}
	return strings.Join(parts, " ")
}
