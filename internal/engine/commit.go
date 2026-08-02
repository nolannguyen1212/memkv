package engine

import (
	"fmt"

	"github.com/william1nguyen/memkv/internal/mutation"
)

func (ctx *Context) Commit(command mutation.Command, apply func()) error {
	victims := ctx.capacityVictims(command)
	batch := mutation.Batch{command}

	for _, key := range victims {
		batch = append(batch, mutation.New("DEL", key))
	}

	if err := ctx.appendWAL(batch); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}

	apply()

	for _, key := range victims {
		ctx.Store.DeleteKey(key)
	}

	ctx.propagate(batch)
	return nil
}

func (ctx *Context) CommitBatch(batch mutation.Batch, apply func()) error {
	if len(batch) == 0 {
		apply()
		return nil
	}

	if err := ctx.appendWAL(batch); err != nil {
		ctx.persistenceFailed.Store(true)
		return err
	}

	apply()
	ctx.propagate(batch)

	return nil
}

func (ctx *Context) capacityVictims(command mutation.Command) []string {
	if !ctx.deriveCapacity || len(command.Args) == 0 {
		return nil
	}

	if !createsKey(command.Name) {
		return nil
	}

	return ctx.Store.Capacity.PlanInsert(command.Args[0])
}

func createsKey(name string) bool {
	switch name {
	case "SET", "LPUSH", "RPUSH", "HSET", "SADD", "ZADD":
		return true
	default:
		return false
	}
}

func (ctx *Context) appendWAL(batch mutation.Batch) error {
	if ctx.WAL == nil {
		return nil
	}

	if appender, ok := ctx.WAL.(WALBatchAppender); ok {
		return appender.AppendBatch(batch)
	}

	if len(batch) == 1 {
		return ctx.WAL.Append(batch[0])
	}

	return fmt.Errorf("WAL does not support atomic batches")
}

func (ctx *Context) propagate(batch mutation.Batch) {
	if ctx.Replication != nil {
		ctx.Replication.Propagate(batch)
	}
}
