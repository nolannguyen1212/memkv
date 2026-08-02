package engine

import (
	"context"
	"errors"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

const requestQueueCapacity = 1024

var ErrStopped = errors.New("engine stopped")

type commandRequest struct {
	connection *ConnContext
	command    Command
	reply      chan resp.Value
}

type restoreRequest struct {
	state store.LogicalSnapshot
	reply chan error
}

type batchRequest struct {
	commands []QueuedCommand
	reply    chan error
}

type rewriteRequest struct {
	path  string
	reply chan error
}

type Checkpoint struct {
	State     store.LogicalSnapshot
	WALOffset int64
}

type checkpointReply struct {
	checkpoint Checkpoint
	err        error
}

type event struct {
	command    *commandRequest
	snapshot   chan store.LogicalSnapshot
	restore    *restoreRequest
	batch      *batchRequest
	rewrite    *rewriteRequest
	checkpoint chan checkpointReply
	disconnect uint64
}

func (ctx *Context) Checkpoint() (Checkpoint, error) {
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return ctx.checkpointOwned()
	}
	reply := make(chan checkpointReply, 1)
	select {
	case ctx.events <- event{checkpoint: reply}:
	case <-ctx.done:
		return Checkpoint{}, ErrStopped
	}
	select {
	case result := <-reply:
		return result.checkpoint, result.err
	case <-ctx.done:
		return Checkpoint{}, ErrStopped
	}
}

func (ctx *Context) checkpointOwned() (Checkpoint, error) {
	offsetter, ok := ctx.WAL.(WALOffsetter)
	if !ok {
		return Checkpoint{State: ctx.Store.Snapshot()}, nil
	}
	offset, err := offsetter.Offset()
	if err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{State: ctx.Store.Snapshot(), WALOffset: offset}, nil
}

func (ctx *Context) Run(runContext context.Context) error {
	if !ctx.running.CompareAndSwap(false, true) {
		return errors.New("engine already running")
	}
	close(ctx.ready)
	defer close(ctx.done)

	ticker := time.NewTicker(ctx.Store.ExpirationCheckInterval())
	defer ticker.Stop()
	for {
		select {
		case <-runContext.Done():
			return nil
		case <-ticker.C:
			ctx.Store.Maintain(ctx.Store.Now())
		case current := <-ctx.events:
			ctx.handleEvent(current)
		}
	}
}

func (ctx *Context) Ready() <-chan struct{} {
	return ctx.ready
}

func (ctx *Context) Submit(connection *ConnContext, name string, args []resp.Value) (resp.Value, error) {
	return ctx.SubmitCommand(connection, NewCommand(name, args))
}

func (ctx *Context) SubmitCommand(connection *ConnContext, command Command) (resp.Value, error) {
	request := &commandRequest{
		connection: connection,
		command:    command,
		reply:      make(chan resp.Value, 1),
	}
	select {
	case ctx.events <- event{command: request}:
	case <-ctx.done:
		return resp.Value{}, ErrStopped
	}
	select {
	case reply := <-request.reply:
		return reply, nil
	case <-ctx.done:
		return resp.Value{}, ErrStopped
	}
}

func (ctx *Context) Snapshot() (store.LogicalSnapshot, error) {
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return ctx.Store.Snapshot(), nil
	}
	reply := make(chan store.LogicalSnapshot, 1)
	select {
	case ctx.events <- event{snapshot: reply}:
	case <-ctx.done:
		return store.LogicalSnapshot{}, ErrStopped
	}
	select {
	case state := <-reply:
		return state, nil
	case <-ctx.done:
		return store.LogicalSnapshot{}, ErrStopped
	}
}

func (ctx *Context) Disconnect(connectionID uint64) {
	if !ctx.running.Load() {
		ctx.watches.unwatch(connectionID)
		return
	}
	select {
	case ctx.events <- event{disconnect: connectionID}:
	case <-ctx.done:
	}
}

func (ctx *Context) ApplyBatch(commands []QueuedCommand) error {
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return ctx.applyBatchOwned(commands)
	}
	request := &batchRequest{commands: commands, reply: make(chan error, 1)}
	select {
	case ctx.events <- event{batch: request}:
	case <-ctx.done:
		return ErrStopped
	}
	select {
	case err := <-request.reply:
		return err
	case <-ctx.done:
		return ErrStopped
	}
}

func (ctx *Context) RewriteWAL(path string) error {
	rewriter, ok := ctx.WAL.(WALRewriter)
	if !ok {
		return errors.New("WAL does not support rewrite")
	}
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return rewriter.RewriteAll(ctx.Store.Snapshot(), path)
	}
	request := &rewriteRequest{path: path, reply: make(chan error, 1)}
	select {
	case ctx.events <- event{rewrite: request}:
	case <-ctx.done:
		return ErrStopped
	}
	select {
	case err := <-request.reply:
		return err
	case <-ctx.done:
		return ErrStopped
	}
}

func (ctx *Context) handleEvent(current event) {
	switch {
	case current.command != nil:
		ctx.Store.Maintain(ctx.Store.Now())
		current.command.reply <- executeOwned(current.command.connection, current.command.command.Name, current.command.command.Args)
	case current.snapshot != nil:
		current.snapshot <- ctx.Store.Snapshot()
	case current.restore != nil:
		current.restore.reply <- ctx.Store.Restore(current.restore.state, ctx.Store.Now())
	case current.batch != nil:
		current.batch.reply <- ctx.applyBatchOwned(current.batch.commands)
	case current.rewrite != nil:
		rewriter := ctx.WAL.(WALRewriter)
		current.rewrite.reply <- rewriter.RewriteAll(ctx.Store.Snapshot(), current.rewrite.path)
	case current.checkpoint != nil:
		checkpoint, err := ctx.checkpointOwned()
		current.checkpoint <- checkpointReply{checkpoint: checkpoint, err: err}
	case current.disconnect != 0:
		ctx.watches.unwatch(current.disconnect)
	}
}
