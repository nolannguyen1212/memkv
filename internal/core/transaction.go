package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

type TxStatus uint8

const (
	TxIdle TxStatus = iota
	TxQueueing
)

type QueueCMD struct {
	Name string
	Args []protocol.Value
}

type TxState struct {
	Status  TxStatus
	Queue   []QueueCMD
	Dirty   bool
	Watches map[string]uint64
}

func (tx *TxState) Reset() {
	tx.Status = TxIdle
	tx.Queue = tx.Queue[:0]
	tx.Dirty = false
	tx.Watches = nil
}

func (tx *TxState) Enqueue(name string, args []protocol.Value) {
	tx.Queue = append(tx.Queue, QueueCMD{Name: name, Args: args})
}
