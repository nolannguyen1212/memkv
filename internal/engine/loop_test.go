package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/william1nguyen/memkv/internal/store"
)

func startTestEngine(t *testing.T) (*Context, context.CancelFunc, <-chan error) {
	t.Helper()
	base := NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{})
	runContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- base.Run(runContext)
	}()
	<-base.Ready()
	return base, cancel, done
}

func stopTestEngine(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEngineLoopCommandSnapshotAndShutdown(t *testing.T) {
	base, cancel, done := startTestEngine(t)
	connection := NewConnContext(base, nil)

	if result := exec(connection, "SET", "key", "value"); result.Type != ResultSimpleString {
		t.Fatalf("SET returned %#v", result)
	}

	state, err := base.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	if state.Strings["key"].Value != "value" {
		t.Fatalf("snapshot value = %q, want value", state.Strings["key"].Value)
	}

	stopTestEngine(t, cancel, done)

	if _, err := base.Submit(connection, "PING", nil); !errors.Is(err, ErrStopped) {
		t.Fatalf("Submit() error = %v, want ErrStopped", err)
	}
}

func TestSubmitBeforeEngineStartsReturnsError(t *testing.T) {
	base := NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{})
	connection := NewConnContext(base, nil)

	if _, err := base.Submit(connection, "PING", nil); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Submit() error = %v, want ErrNotStarted", err)
	}
}

func TestBootstrapExecutionIsRejectedAfterEngineStarts(t *testing.T) {
	base, cancel, done := startTestEngine(t)
	defer stopTestEngine(t, cancel, done)

	result := BootstrapExecute(NewConnContext(base, nil), "SET", []string{"key", "value"})

	if result.Type != ResultError {
		t.Fatalf("BootstrapExecute returned %#v, want error", result)
	}

	state, err := base.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	if _, exists := state.Strings["key"]; exists {
		t.Fatal("bootstrap execution mutated a running engine")
	}
}

func TestMultipleEngineLoopsRemainIsolated(t *testing.T) {
	first, cancelFirst, doneFirst := startTestEngine(t)
	second, cancelSecond, doneSecond := startTestEngine(t)
	defer stopTestEngine(t, cancelFirst, doneFirst)
	defer stopTestEngine(t, cancelSecond, doneSecond)

	exec(NewConnContext(first, nil), "SET", "key", "first")
	exec(NewConnContext(second, nil), "SET", "key", "second")
	firstState, err := first.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	secondState, err := second.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	if firstState.Strings["key"].Value != "first" || secondState.Strings["key"].Value != "second" {
		t.Fatalf(
			"engine states leaked: first=%q second=%q",
			firstState.Strings["key"].Value,
			secondState.Strings["key"].Value,
		)
	}
}
