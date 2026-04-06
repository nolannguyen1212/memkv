package core

import "github.com/william1nguyen/valkeydb/internal/protocol"

type TransactionStatus uint8

const (
	TransactionIdle    TransactionStatus = iota
	TransactionQueuing TransactionStatus = iota
)

type QueuedCommand struct {
	Name string
	Args []protocol.Value
}

type TransactionState struct {
	Status  TransactionStatus
	Queue   []QueuedCommand
	Dirty   bool
	Watches map[string]uint64
}

func (transaction *TransactionState) Reset() {
	transaction.Status = TransactionIdle
	transaction.Queue = transaction.Queue[:0]
	transaction.Dirty = false
	transaction.Watches = nil
}

func (transaction *TransactionState) Enqueue(name string, args []protocol.Value) {
	transaction.Queue = append(transaction.Queue, QueuedCommand{Name: name, Args: args})
}
