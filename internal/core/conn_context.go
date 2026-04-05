package core

import "sync/atomic"

type ConnContext struct {
	*Context
	ID uint64
	TX TxState
}

var connCounter atomic.Uint64

func NewConnContext(base *Context) *ConnContext {
	return &ConnContext{
		Context: base,
		ID:      connCounter.Add(1),
	}
}
