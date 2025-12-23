package command

import (
	"strings"
	"sync"

	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

type Handler func(dataStore *store.Store, arguments []protocol.Value) protocol.Value

type ExecutionContext struct {
	Store          *store.Store
	AppendToLog    func(protocol.Value)
	SubscribeChan  chan protocol.Value
	SubscribedName string
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
	handler, exists := handlers[strings.ToUpper(name)]
	return handler, exists
}

func Execute(dataStore *store.Store, name string, arguments []protocol.Value) protocol.Value {
	handler, exists := Lookup(name)
	if !exists {
		return ErrorResponse("ERR unknown command '" + name + "'")
	}
	return handler(dataStore, arguments)
}
