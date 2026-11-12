package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nolannguyen1212/memkv/internal/mutation"
)

func openCounting(t *testing.T, policy SyncPolicy) (*Log, *atomic.Int64) {
	t.Helper()
	log, err := OpenWithPolicy(filepath.Join(t.TempDir(), "memkv.wal"), true, policy)

	if err != nil {
		t.Fatal(err)
	}

	var syncs atomic.Int64
	log.mutex.Lock()
	log.syncFile = func(file *os.File) error {
		syncs.Add(1)
		return file.Sync()
	}
	log.mutex.Unlock()
	t.Cleanup(func() { _ = log.Close() })
	return log, &syncs
}

func setCommand(index int) mutation.Command {
	return mutation.New("SET", "key", string(rune('a'+index%26)))
}

func TestParseSyncPolicy(t *testing.T) {
	for _, name := range []string{"always", "everysec", "no"} {
		if _, err := ParseSyncPolicy(name); err != nil {
			t.Fatalf("ParseSyncPolicy(%q) error = %v", name, err)
		}
	}

	if _, err := ParseSyncPolicy("sometimes"); err == nil {
		t.Fatal("ParseSyncPolicy(sometimes) error = nil, want error")
	}
}

func TestOpenRejectsUnknownPolicy(t *testing.T) {
	if _, err := OpenWithPolicy(filepath.Join(t.TempDir(), "memkv.wal"), true, "sometimes"); err == nil {
		t.Fatal("OpenWithPolicy() error = nil, want error")
	}
}

func TestAlwaysPolicySyncsEveryAppend(t *testing.T) {
	log, syncs := openCounting(t, SyncAlways)

	for index := 0; index < 5; index++ {
		if err := log.Append(setCommand(index)); err != nil {
			t.Fatal(err)
		}
	}

	if got := syncs.Load(); got != 5 {
		t.Fatalf("syncs = %d, want 5", got)
	}
}

func TestDeferredAppendsShareOneSync(t *testing.T) {
	log, syncs := openCounting(t, SyncAlways)

	for index := 0; index < 5; index++ {
		if err := log.AppendBatchDeferred(mutation.Batch{setCommand(index)}); err != nil {
			t.Fatal(err)
		}
	}

	if got := syncs.Load(); got != 0 {
		t.Fatalf("syncs before Sync = %d, want 0", got)
	}

	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}

	if got := syncs.Load(); got != 1 {
		t.Fatalf("syncs = %d, want 1 (second Sync has nothing pending)", got)
	}
}

func TestNoPolicySyncsOnlyOnClose(t *testing.T) {
	log, syncs := openCounting(t, SyncNo)

	for index := 0; index < 5; index++ {
		if err := log.Append(setCommand(index)); err != nil {
			t.Fatal(err)
		}
	}

	if got := syncs.Load(); got != 0 {
		t.Fatalf("syncs before Close = %d, want 0", got)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	if got := syncs.Load(); got != 1 {
		t.Fatalf("syncs after Close = %d, want 1", got)
	}
}

func TestEverySecPolicyBatchesSyncsInBackground(t *testing.T) {
	previous := everySecInterval
	everySecInterval = 20 * time.Millisecond
	t.Cleanup(func() { everySecInterval = previous })

	log, syncs := openCounting(t, SyncEverySec)
	const appends = 200

	for index := 0; index < appends; index++ {
		if err := log.Append(setCommand(index)); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)

	for syncs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := syncs.Load(); got == 0 || got >= appends {
		t.Fatalf("syncs = %d, want between 1 and %d", got, appends-1)
	}
}

func TestEverySecSyncFailureFailsNextAppend(t *testing.T) {
	previous := everySecInterval
	everySecInterval = 10 * time.Millisecond
	t.Cleanup(func() { everySecInterval = previous })

	log, _ := openCounting(t, SyncEverySec)
	failure := errors.New("disk full")
	log.mutex.Lock()
	log.syncFile = func(*os.File) error { return failure }
	log.mutex.Unlock()

	if err := log.Append(setCommand(0)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if err := log.Append(setCommand(1)); errors.Is(err, failure) {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("Append() never reported the background sync failure")
}

func TestCloseTwiceDoesNotPanic(t *testing.T) {
	log, _ := openCounting(t, SyncEverySec)

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	_ = log.Close()
}
