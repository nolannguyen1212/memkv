package wal_test

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/nolannguyen1212/memkv/internal/engine"
	"github.com/nolannguyen1212/memkv/internal/mutation"
	wallog "github.com/nolannguyen1212/memkv/internal/wal"
)

func TestGroupCommitAcknowledgedWritesSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memkv.wal")
	log, err := wallog.OpenWithPolicy(path, true, wallog.SyncAlways)

	if err != nil {
		t.Fatal(err)
	}

	base := engine.NewContext(newStore(), log, nil, engine.SystemConfig{WALEnabled: true})
	base.EnableGroupCommit(engine.DefaultGroupCommitSize)
	runContext, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- base.Run(runContext) }()
	<-base.Ready()

	const clients = 16
	const writesPerClient = 40
	var group sync.WaitGroup

	for client := 0; client < clients; client++ {
		group.Add(1)

		go func() {
			defer group.Done()
			connection := engine.NewConnContext(base, nil)

			for write := 0; write < writesPerClient; write++ {
				key := "key-" + strconv.Itoa(client) + "-" + strconv.Itoa(write)
				result, err := base.Submit(connection, "SET", []string{key, key})

				if err != nil || result.Type != engine.ResultSimpleString {
					t.Errorf("SET %s = %#v, %v", key, result, err)
					return
				}
			}
		}()
	}

	group.Wait()
	cancel()

	if err := <-stopped; err != nil {
		t.Fatal(err)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := engine.NewConnContext(engine.NewContext(newStore(), nil, nil, engine.SystemConfig{}), nil)
	loader, err := wallog.Open(path, true)

	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = loader.Close() }()

	if err := loader.Load(path, func(command mutation.Command) error {
		engine.BootstrapExecute(recovered, command.Name, command.Args)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for client := 0; client < clients; client++ {
		for write := 0; write < writesPerClient; write++ {
			key := "key-" + strconv.Itoa(client) + "-" + strconv.Itoa(write)

			if value, exists := recovered.Store.Dictionary.Get(key); !exists || value != key {
				t.Fatalf("recovered %s = %q (exists=%v), want %q", key, value, exists, key)
			}
		}
	}
}
