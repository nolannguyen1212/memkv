package engine

import (
	"sync/atomic"

	"github.com/nolannguyen1212/memkv/internal/mutation"
)

const DefaultGroupCommitSize = 256

type WALDeferredAppender interface {
	AppendBatchDeferred(batch mutation.Batch) error
	Sync() error
}

type pendingReply struct {
	reply   chan Result
	result  Result
	durable bool
}

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
