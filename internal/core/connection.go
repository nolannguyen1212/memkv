package core

import (
	"net"
	"sync/atomic"
)

type ConnContext struct {
	*Context
	ID                     uint64
	Connection             net.Conn
	Transaction            TransactionState
	ReplicaAddress         string
	ReplicaAPIKeyValidated bool
}

var connCounter atomic.Uint64

func NewConnContext(base *Context, connection net.Conn) *ConnContext {
	return &ConnContext{
		Context:    base,
		ID:         connCounter.Add(1),
		Connection: connection,
	}
}
