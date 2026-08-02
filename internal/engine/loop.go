package engine

import (
	"context"
	"errors"
	"time"

	"github.com/william1nguyen/valkeydb/internal/store"
)

const requestQueueCapacity = 1024

var (
	ErrNotStarted = errors.New("engine not started")
	ErrStopped    = errors.New("engine stopped")
)

type commandRequest struct {
	connection *ConnContext
	command    Command
	reply      chan Result
}

func (request commandRequest) handle(ctx *Context) {
	ctx.Store.Maintain(ctx.Store.Now())
	request.reply <- executeOwned(
		request.connection,
		request.command.Name,
		request.command.Args,
	)
}

type snapshotRequest struct {
	reply chan store.LogicalSnapshot
}

func (request snapshotRequest) handle(ctx *Context) {
	request.reply <- ctx.Store.Snapshot()
}

type restoreRequest struct {
	state store.LogicalSnapshot
	reply chan error
}

func (request restoreRequest) handle(ctx *Context) {
	request.reply <- ctx.Store.Restore(request.state, ctx.Store.Now())
}

type batchRequest struct {
	commands []QueuedCommand
	reply    chan error
}

func (request batchRequest) handle(ctx *Context) {
	request.reply <- ctx.applyBatchOwned(request.commands)
}

type rewriteRequest struct {
	path  string
	reply chan error
}

func (request rewriteRequest) handle(ctx *Context) {
	rewriter := ctx.WAL.(WALRewriter)
	request.reply <- rewriter.RewriteAll(ctx.Store.Snapshot(), request.path)
}

type Checkpoint struct {
	State     store.LogicalSnapshot
	WALOffset int64
}

type checkpointReply struct {
	checkpoint Checkpoint
	err        error
}

type replicationSnapshotReply struct {
	state  store.LogicalSnapshot
	offset int64
}

type checkpointRequest struct {
	reply chan checkpointReply
}

func (request checkpointRequest) handle(ctx *Context) {
	checkpoint, err := ctx.checkpointOwned()
	request.reply <- checkpointReply{checkpoint: checkpoint, err: err}
}

type replicationSnapshotRequest struct {
	reply chan replicationSnapshotReply
}

func (request replicationSnapshotRequest) handle(ctx *Context) {
	request.reply <- replicationSnapshotReply{
		state:  ctx.Store.Snapshot(),
		offset: ctx.replicationOffset(),
	}
}

type disconnectRequest struct {
	connectionID uint64
}

func (request disconnectRequest) handle(ctx *Context) {
	ctx.watches.unwatch(request.connectionID)
}

type engineEvent interface {
	handle(*Context)
}

func (ctx *Context) captureReplicationSnapshot() (store.LogicalSnapshot, int64, error) {
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return ctx.Store.Snapshot(), ctx.replicationOffset(), nil
	}

	reply := make(chan replicationSnapshotReply, 1)

	select {
	case ctx.events <- replicationSnapshotRequest{reply: reply}:
	case <-ctx.done:
		return store.LogicalSnapshot{}, 0, ErrStopped
	}

	select {
	case result := <-reply:
		return result.state, result.offset, nil
	case <-ctx.done:
		return store.LogicalSnapshot{}, 0, ErrStopped
	}
}

func (ctx *Context) replicationOffset() int64 {
	if ctx.Replication == nil {
		return 0
	}

	return ctx.Replication.Offset()
}

func (ctx *Context) Checkpoint() (Checkpoint, error) {
	if !ctx.running.Load() {
		ctx.directMu.Lock()
		defer ctx.directMu.Unlock()
		return ctx.checkpointOwned()
	}

	reply := make(chan checkpointReply, 1)

	select {
	case ctx.events <- checkpointRequest{reply: reply}:
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
	ctx.directMu.Lock()

	if !ctx.running.CompareAndSwap(false, true) {
		ctx.directMu.Unlock()
		return errors.New("engine already running")
	}

	ctx.directMu.Unlock()
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
			current.handle(ctx)
		}
	}
}

func (ctx *Context) Ready() <-chan struct{} {
	return ctx.ready
}

func (ctx *Context) Submit(connection *ConnContext, name string, args []string) (Result, error) {
	return ctx.SubmitCommand(connection, NewCommand(name, args))
}

func (ctx *Context) SubmitCommand(connection *ConnContext, command Command) (Result, error) {
	if !ctx.running.Load() {
		return Result{}, ErrNotStarted
	}

	request := &commandRequest{
		connection: connection,
		command:    command,
		reply:      make(chan Result, 1),
	}

	select {
	case ctx.events <- *request:
	case <-ctx.done:
		return Result{}, ErrStopped
	}

	select {
	case reply := <-request.reply:
		return reply, nil
	case <-ctx.done:
		return Result{}, ErrStopped
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
	case ctx.events <- snapshotRequest{reply: reply}:
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
	case ctx.events <- disconnectRequest{connectionID: connectionID}:
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
	case ctx.events <- *request:
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
	case ctx.events <- *request:
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
