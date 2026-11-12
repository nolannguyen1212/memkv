package engine

import (
	"sync/atomic"

	"github.com/nolannguyen1212/memkv/internal/mutation"
)

// DefaultGroupCommitSize bounds how many queued commands share one fsync, so a
// long queue cannot delay the first client's reply without limit.
const DefaultGroupCommitSize = 256

// WALDeferredAppender is a WAL that can append without syncing and sync later.
type WALDeferredAppender interface {
	AppendBatchDeferred(batch mutation.Batch) error
	Sync() error
}

type pendingReply struct {
	reply   chan Result
	result  Result
	durable bool
}

// groupCommit lets the engine run several queued commands, fsync the WAL once,
// and only then reply to every client, so replies still follow durability.
//
// It is owned by the engine goroutine and needs no locking. Mutations are
// applied before the fsync, so if the fsync fails the in-memory state holds
// writes that were never acknowledged. The engine then rejects further writes
// through persistenceFailed, and recovery restarts from the WAL.
type groupCommit struct {
	limit   int
	active  bool
	dirty   bool
	appends int
	replies []pendingReply
	batches []mutation.Batch
	syncs   atomic.Uint64
	writes  atomic.Uint64
}

// EnableGroupCommit turns on group commit. It must be called before Run and is
// a no-op when the WAL cannot append without syncing.
func (ctx *Context) EnableGroupCommit(limit int) {
	if _, ok := ctx.WAL.(WALDeferredAppender); !ok || limit < 1 {
		return
	}

	ctx.group = &groupCommit{limit: limit}
}

func (ctx *Context) groupActive() bool {
	return ctx.group != nil && ctx.group.active
}

func (ctx *Context) groupAppends() int {
	if ctx.group == nil {
		return 0
	}

	return ctx.group.appends
}

func (ctx *Context) appendDeferred(batch mutation.Batch) (bool, error) {
	if !ctx.groupActive() {
		return false, nil
	}

	deferred, ok := ctx.WAL.(WALDeferredAppender)

	if !ok {
		return false, nil
	}

	if err := deferred.AppendBatchDeferred(batch); err != nil {
		return true, err
	}

	ctx.group.dirty = true
	ctx.group.appends++
	return true, nil
}

// respond replies immediately unless the group holds unsynced writes. Replies
// stay ordered behind those writes so no client observes data that a failed
// fsync would lose.
func (ctx *Context) respond(reply chan Result, result Result, appendsBefore int) {
	group := ctx.group

	if group == nil || !group.active || !group.dirty {
		reply <- result
		return
	}

	group.replies = append(group.replies, pendingReply{
		reply:   reply,
		result:  result,
		durable: group.appends > appendsBefore,
	})
}

func (ctx *Context) dispatch(event engineEvent) {
	if ctx.group == nil {
		event.handle(ctx)
		return
	}

	for handled := 1; ; handled++ {
		request, isCommand := event.(commandRequest)

		if !isCommand {
			ctx.flushGroup()
			event.handle(ctx)
			return
		}

		ctx.group.active = true
		request.handle(ctx)
		ctx.group.active = false

		if handled >= ctx.group.limit {
			break
		}

		select {
		case event = <-ctx.events:
		default:
			ctx.flushGroup()
			return
		}
	}

	ctx.flushGroup()
}

func (ctx *Context) flushGroup() {
	group := ctx.group
	var syncErr error

	if group.dirty {
		syncErr = ctx.WAL.(WALDeferredAppender).Sync()
		group.syncs.Add(1)
		group.writes.Add(uint64(group.appends))

		if syncErr != nil {
			ctx.persistenceFailed.Store(true)
		}
	}

	if syncErr == nil {
		for _, batch := range group.batches {
			ctx.propagate(batch)
		}
	}

	for _, pending := range group.replies {
		if syncErr != nil && pending.durable {
			pending.reply <- persistenceError()
			continue
		}

		pending.reply <- pending.result
	}

	group.dirty = false
	group.appends = 0
	group.replies = group.replies[:0]
	group.batches = group.batches[:0]
}
