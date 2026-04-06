package core

import (
	"strings"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

type Handler func(connContext *ConnContext, args []protocol.Value) protocol.Value

type AOFAppender interface {
	Append(value protocol.Value) error
}

type Context struct {
	Store *Store
	AOF   AOFAppender
}

func NewContext(store *Store, aof AOFAppender) *Context {
	return &Context{Store: store, AOF: aof}
}

func (ctx *Context) OnKeyMutate(key string) {
	globalWatch.Notify(key)
}

func (ctx *Context) OnKeyWrite(key string) {
	ctx.OnKeyMutate(key)
	ctx.Store.Eviction.RecordInsert(key)
	for ctx.Store.Eviction.ShouldEvict() {
		if ctx.Store.Eviction.EvictOne() == "" {
			break
		}
	}
}

func (ctx *Context) OnKeyRead(key string) {
	ctx.Store.Eviction.RecordAccess(key)
}

func (ctx *Context) OnKeyDelete(key string) {
	ctx.OnKeyMutate(key)
	ctx.Store.Eviction.RecordDelete(key)
}

func (ctx *Context) AppendAOF(value protocol.Value) {
	if ctx.AOF != nil {
		_ = ctx.AOF.Append(value)
	}
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
	"MULTI":   true,
	"EXEC":    true,
	"DISCARD": true,
	"WATCH":   true,
	"UNWATCH": true,
	"QUIT":    true,
}

func Execute(connContext *ConnContext, name string, args []protocol.Value) protocol.Value {
	if connContext.Transaction.Status == TransactionQueuing && !txPassthrough[name] {
		connContext.Transaction.Enqueue(name, args)
		return protocol.Value{Type: protocol.TypeSimpleString, String: "QUEUED"}
	}
	handler, exists := Lookup(name)
	if !exists {
		return ErrorResponse("ERR unknown command '" + name + "'")
	}
	return handler(connContext, args)
}
