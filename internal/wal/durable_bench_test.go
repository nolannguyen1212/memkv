package wal_test

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/nolannguyen1212/memkv/internal/engine"
	wallog "github.com/nolannguyen1212/memkv/internal/wal"
)

const benchmarkClients = 50

func benchmarkDurableSet(b *testing.B, policy wallog.SyncPolicy, groupCommit bool) {
	b.Helper()
	log, err := wallog.OpenWithPolicy(filepath.Join(b.TempDir(), "memkv.wal"), true, policy)

	if err != nil {
		b.Fatal(err)
	}

	base := engine.NewContext(newStore(), log, nil, engine.SystemConfig{WALEnabled: true})

	if groupCommit {
		base.EnableGroupCommit(engine.DefaultGroupCommitSize)
	}

	runContext, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- base.Run(runContext) }()
	<-base.Ready()

	var next atomic.Uint64
	b.SetParallelism(max(1, benchmarkClients/runtime.GOMAXPROCS(0)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		connection := engine.NewConnContext(base, nil)
		key := "key-" + strconv.FormatUint(next.Add(1), 10)

		for pb.Next() {
			result, err := base.Submit(connection, "SET", []string{key, "value"})

			if err != nil || result.Type != engine.ResultSimpleString {
				b.Errorf("SET = %#v, %v", result, err)
				return
			}
		}
	})

	b.StopTimer()
	cancel()

	if err := <-stopped; err != nil {
		b.Fatal(err)
	}

	if err := log.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDurableSetFsyncAlways(b *testing.B) {
	benchmarkDurableSet(b, wallog.SyncAlways, false)
}

func BenchmarkDurableSetFsyncAlwaysGroupCommit(b *testing.B) {
	benchmarkDurableSet(b, wallog.SyncAlways, true)
}

func BenchmarkDurableSetFsyncEverySec(b *testing.B) {
	benchmarkDurableSet(b, wallog.SyncEverySec, false)
}

func BenchmarkDurableSetFsyncNo(b *testing.B) {
	benchmarkDurableSet(b, wallog.SyncNo, false)
}
