package engine

import (
	"net"
)

type ConnContext struct {
	*Context
	ID             uint64
	Connection     net.Conn
	Transaction    TransactionState
	ReplicaAddress string
}

func NewConnContext(base *Context, connection net.Conn) *ConnContext {
	return &ConnContext{
		Context:    base,
		ID:         base.connectionCounter.Add(1),
		Connection: connection,
	}
}
