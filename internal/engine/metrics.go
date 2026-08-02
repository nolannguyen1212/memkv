package engine

import (
	"sync/atomic"
	"time"
)

type metrics struct {
	startedAt        time.Time
	totalCommands    atomic.Uint64
	totalConnections atomic.Uint64
	openConnections  atomic.Int64
}

func newMetrics() *metrics {
	return &metrics{startedAt: time.Now()}
}

func (metrics *metrics) connectionOpened() {
	metrics.totalConnections.Add(1)
	metrics.openConnections.Add(1)
}

func (metrics *metrics) connectionClosed() {
	metrics.openConnections.Add(-1)
}

func (metrics *metrics) uptime() time.Duration {
	return time.Since(metrics.startedAt)
}
