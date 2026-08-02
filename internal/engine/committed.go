package engine

import (
	"fmt"

	"github.com/william1nguyen/valkeydb/internal/mutation"
)

type transactionWAL struct {
	values mutation.Batch
}

func (log *transactionWAL) Append(command mutation.Command) error {
	log.values = append(log.values, command)
	return nil
}

func (ctx *Context) Replay(commands []QueuedCommand) error {
	ctx.directMu.Lock()
	defer ctx.directMu.Unlock()

	if len(commands) == 1 {
		return ctx.applyCommittedCommand(commands[0])
	}

	now := ctx.Store.Now()
	stagedStore, err := ctx.Store.Clone(now)

	if err != nil {
		return err
	}

	stagedContext := newContext(stagedStore, &transactionWAL{}, nil, ctx.system, false)
	stagedContext.Registry = ctx.Registry
	connection := NewConnContext(stagedContext, nil)

	for _, command := range commands {
		result := executeCommand(connection, command.Name, command.Args)

		if result.Type == ResultError {
			return fmt.Errorf("execute %s: %s", command.Name, result.String)
		}
	}

	return ctx.Store.Restore(stagedStore.Snapshot(), now)
}

func (ctx *Context) applyBatchOwned(commands []QueuedCommand) error {
	if len(commands) == 1 {
		return ctx.applyCommittedCommand(commands[0])
	}

	now := ctx.Store.Now()
	stagedStore, err := ctx.Store.Clone(now)

	if err != nil {
		return err
	}

	stagedWAL := &transactionWAL{}
	stagedContext := newContext(stagedStore, stagedWAL, nil, ctx.system, false)
	stagedContext.Registry = ctx.Registry
	connection := NewConnContext(stagedContext, nil)

	for _, command := range commands {
		result := executeCommand(connection, command.Name, command.Args)

		if result.Type == ResultError {
			return fmt.Errorf("execute %s: %s", command.Name, result.String)
		}
	}

	before := ctx.Store.Snapshot()
	after := stagedStore.Snapshot()
	return ctx.CommitBatch(stagedWAL.values, func() {
		if err := ctx.Store.Restore(after, now); err != nil {
			panic(err)
		}

		notifyChangedVersions(ctx.watches, before.Versions, after.Versions)
	})
}

func (ctx *Context) applyCommittedCommand(command QueuedCommand) error {
	deriveCapacity := ctx.deriveCapacity
	ctx.deriveCapacity = false
	defer func() {
		ctx.deriveCapacity = deriveCapacity
	}()

	connection := NewConnContext(ctx, nil)
	result := executeCommand(connection, command.Name, command.Args)

	if result.Type == ResultError {
		return fmt.Errorf("execute %s: %s", command.Name, result.String)
	}

	return nil
}

func notifyChangedVersions(watches *watchRegistry, before, after map[string]uint64) {
	for key, version := range after {
		if before[key] != version {
			watches.notify(key)
		}
	}

	for key := range before {
		if _, exists := after[key]; !exists {
			watches.notify(key)
		}
	}
}
