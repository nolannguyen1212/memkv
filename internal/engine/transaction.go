package engine

type TransactionStatus uint8

const (
	TransactionIdle    TransactionStatus = iota
	TransactionQueuing TransactionStatus = iota
)

type QueuedCommand = Command

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

func (transaction *TransactionState) Enqueue(name string, args []string) {
	transaction.Queue = append(transaction.Queue, Command{Name: name, Args: append([]string(nil), args...)})
}

func registerTransactionCommands(registry *Registry) {
	registry.register("MULTI", handleMulti, arguments(0, 0), transactionControl)
	registry.register("EXEC", handleExec, arguments(0, 0), transactionControl)
	registry.register("DISCARD", handleDiscard, arguments(0, 0), transactionControl)
	registry.register("WATCH", handleWatch, arguments(1, -1), transactionControl)
	registry.register("UNWATCH", handleUnwatch, arguments(0, 0), transactionControl)
}

func handleMulti(connContext *ConnContext, _ []string) Result {
	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR MULTI calls can not be nested")
	}

	connContext.Transaction.Status = TransactionQueuing
	return okReply()
}

func handleExec(connContext *ConnContext, _ []string) Result {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR EXEC without MULTI")
	}

	defer connContext.finishTransaction()

	if connContext.Transaction.Dirty {
		return errorReply("EXECABORT Transaction discarded because of previous errors.")
	}

	if connContext.watches.isDirty(connContext.ID) {
		return nullArrayReply()
	}

	for key, version := range connContext.Transaction.Watches {
		if connContext.Store.Keyspace.Version(key) != version {
			return nullArrayReply()
		}
	}

	connContext.Transaction.Status = TransactionIdle
	now := connContext.Store.Now()
	stagedStore, err := connContext.Store.Clone(now)

	if err != nil {
		return errorReply("ERR transaction planning failed")
	}

	stagedWAL := &transactionWAL{}
	stagedBase := NewContext(stagedStore, stagedWAL, nil, connContext.system)
	stagedBase.Registry = connContext.Registry
	stagedConnection := NewConnContext(stagedBase, nil)

	results := make([]Result, len(connContext.Transaction.Queue))

	for index, command := range connContext.Transaction.Queue {
		results[index] = executeCommand(stagedConnection, command.Name, command.Args)
	}

	if len(stagedWAL.values) == 0 {
		return Result{Type: ResultArray, Array: results}
	}

	before := connContext.Store.Snapshot()
	after := stagedStore.Snapshot()

	if err := connContext.CommitBatch(stagedWAL.values, func() {
		if err := connContext.Store.Restore(after, now); err != nil {
			panic(err)
		}

		notifyChangedVersions(connContext.watches, before.Versions, after.Versions)
	}); err != nil {
		return persistenceError()
	}

	return Result{Type: ResultArray, Array: results}
}

func (connContext *ConnContext) finishTransaction() {
	connContext.watches.unwatch(connContext.ID)
	connContext.Transaction.Reset()
}

func handleDiscard(connContext *ConnContext, _ []string) Result {
	if connContext.Transaction.Status != TransactionQueuing {
		return errorReply("ERR DISCARD without MULTI")
	}

	connContext.watches.unwatch(connContext.ID)
	connContext.Transaction.Reset()
	return okReply()
}

func handleWatch(connContext *ConnContext, args []string) Result {
	if len(args) == 0 {
		return wrongArgCountError("watch")
	}

	if connContext.Transaction.Status == TransactionQueuing {
		return errorReply("ERR WATCH inside MULTI is not allowed")
	}

	if connContext.Transaction.Watches == nil {
		connContext.Transaction.Watches = make(map[string]uint64)
	}

	for _, arg := range args {
		key := arg
		connContext.Transaction.Watches[key] = connContext.Store.Keyspace.Version(key)
		connContext.watches.watch(key, connContext.ID)
	}

	return okReply()
}

func handleUnwatch(connContext *ConnContext, _ []string) Result {
	connContext.watches.unwatch(connContext.ID)
	connContext.Transaction.Dirty = false
	connContext.Transaction.Watches = nil
	return okReply()
}
