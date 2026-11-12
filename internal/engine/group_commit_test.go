package engine

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nolannguyen1212/memkv/internal/mutation"
	"github.com/nolannguyen1212/memkv/internal/store"
)

type fakeWAL struct {
	mutex       sync.Mutex
	appended    int
	syncs       int
	syncErr     error
	release     chan struct{}
	firstSynced chan struct{}
	signalOnce  sync.Once
}

func (log *fakeWAL) Append(mutation.Command) error {
	return log.AppendBatchDeferred(nil)
}

func (log *fakeWAL) AppendBatch(mutation.Batch) error {
	return log.AppendBatchDeferred(nil)
}

func (log *fakeWAL) AppendBatchDeferred(mutation.Batch) error {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.appended++
	return nil
}

func (log *fakeWAL) Sync() error {
	log.mutex.Lock()
	log.syncs++
	err := log.syncErr
	log.mutex.Unlock()

	if log.firstSynced != nil {
		log.signalOnce.Do(func() {
			close(log.firstSynced)

			if log.release != nil {
				<-log.release
			}
		})
	}

	return err
}

func (log *fakeWAL) counts() (appended, syncs int) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.appended, log.syncs
}

type recordingReplicator struct {
	Replicator
	mutex   sync.Mutex
	batches int
}

func (replicator *recordingReplicator) Propagate(mutation.Batch) {
	replicator.mutex.Lock()
	defer replicator.mutex.Unlock()
	replicator.batches++
}

func (replicator *recordingReplicator) propagated() int {
	replicator.mutex.Lock()
	defer replicator.mutex.Unlock()
	return replicator.batches
}

func startGroupEngine(t *testing.T, log *fakeWAL, replicator Replicator, limit int) *Context {
	t.Helper()
	base := NewContext(store.New(testStoreConfig), log, replicator, SystemConfig{WALEnabled: true})
	base.EnableGroupCommit(limit)
	runContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- base.Run(runContext) }()
	<-base.Ready()
	t.Cleanup(func() {
		cancel()

		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return base
}

func submitSet(base *Context, index int) (Result, error) {
	key := "key" + strconv.Itoa(index)
	return base.Submit(NewConnContext(base, nil), "SET", []string{key, "value"})
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached before timeout")
		}

		time.Sleep(time.Millisecond)
	}
}

func TestGroupCommitSharesOneSyncAcrossQueuedWrites(t *testing.T) {
	log := &fakeWAL{release: make(chan struct{}), firstSynced: make(chan struct{})}
	base := startGroupEngine(t, log, nil, DefaultGroupCommitSize)
	const queued = 20

	var first sync.WaitGroup
	first.Add(1)

	go func() {
		defer first.Done()
		_, _ = submitSet(base, 0)
	}()

	<-log.firstSynced
	results := make(chan Result, queued)

	for index := 1; index <= queued; index++ {
		go func() {
			result, err := submitSet(base, index)

			if err != nil {
				t.Error(err)
			}

			results <- result
		}()
	}

	waitFor(t, func() bool { return len(base.events) == queued })
	close(log.release)
	first.Wait()

	for index := 0; index < queued; index++ {
		if result := <-results; result.Type != ResultSimpleString {
			t.Fatalf("SET returned %#v", result)
		}
	}

	appended, syncs := log.counts()

	if appended != queued+1 {
		t.Fatalf("appended = %d, want %d", appended, queued+1)
	}

	if syncs != 2 {
		t.Fatalf("syncs = %d, want 2 (one for the first write, one shared by %d queued writes)", syncs, queued)
	}
}

func TestGroupCommitRepliesOnlyAfterSync(t *testing.T) {
	log := &fakeWAL{release: make(chan struct{}), firstSynced: make(chan struct{})}
	base := startGroupEngine(t, log, nil, DefaultGroupCommitSize)
	replied := make(chan Result, 1)

	go func() {
		result, _ := submitSet(base, 0)
		replied <- result
	}()

	<-log.firstSynced

	select {
	case result := <-replied:
		t.Fatalf("replied %#v before the WAL sync finished", result)
	case <-time.After(50 * time.Millisecond):
	}

	close(log.release)

	if result := <-replied; result.Type != ResultSimpleString {
		t.Fatalf("SET returned %#v", result)
	}
}

func TestGroupCommitPropagatesOnlyAfterSync(t *testing.T) {
	log := &fakeWAL{release: make(chan struct{}), firstSynced: make(chan struct{})}
	replicator := &recordingReplicator{}
	base := startGroupEngine(t, log, replicator, DefaultGroupCommitSize)
	replied := make(chan Result, 1)

	go func() {
		result, _ := submitSet(base, 0)
		replied <- result
	}()

	<-log.firstSynced

	if got := replicator.propagated(); got != 0 {
		t.Fatalf("propagated %d batches before the WAL sync finished, want 0", got)
	}

	close(log.release)
	<-replied

	if got := replicator.propagated(); got != 1 {
		t.Fatalf("propagated %d batches after sync, want 1", got)
	}
}

func TestGroupCommitSyncFailureRejectsWrites(t *testing.T) {
	log := &fakeWAL{syncErr: errors.New("disk full")}
	base := startGroupEngine(t, log, nil, DefaultGroupCommitSize)

	result, err := submitSet(base, 0)

	if err != nil {
		t.Fatal(err)
	}

	if result.Type != ResultError || result.String != persistenceError().String {
		t.Fatalf("SET after failed sync returned %#v, want persistence error", result)
	}

	again, err := submitSet(base, 1)

	if err != nil {
		t.Fatal(err)
	}

	if again.Type != ResultError {
		t.Fatalf("second SET returned %#v, want rejection after persistence failure", again)
	}
}

func TestGroupCommitReadsDoNotSync(t *testing.T) {
	log := &fakeWAL{}
	base := startGroupEngine(t, log, nil, DefaultGroupCommitSize)
	connection := NewConnContext(base, nil)

	if _, err := base.Submit(connection, "GET", []string{"missing"}); err != nil {
		t.Fatal(err)
	}

	if _, err := base.Submit(connection, "PING", nil); err != nil {
		t.Fatal(err)
	}

	if _, syncs := log.counts(); syncs != 0 {
		t.Fatalf("syncs = %d, want 0 for read-only commands", syncs)
	}
}

func TestGroupCommitRespectsSizeLimit(t *testing.T) {
	log := &fakeWAL{release: make(chan struct{}), firstSynced: make(chan struct{})}
	base := startGroupEngine(t, log, nil, 2)
	const queued = 6

	go func() { _, _ = submitSet(base, 0) }()
	<-log.firstSynced
	var group sync.WaitGroup

	for index := 1; index <= queued; index++ {
		group.Add(1)

		go func() {
			defer group.Done()
			_, _ = submitSet(base, index)
		}()
	}

	waitFor(t, func() bool { return len(base.events) == queued })
	close(log.release)
	group.Wait()

	if _, syncs := log.counts(); syncs < 1+queued/2 {
		t.Fatalf("syncs = %d, want at least %d with a group limit of 2", syncs, 1+queued/2)
	}
}

func TestEnableGroupCommitIgnoresWALWithoutDeferredAppend(t *testing.T) {
	base := NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{})
	base.EnableGroupCommit(DefaultGroupCommitSize)

	if base.group != nil {
		t.Fatal("group commit enabled without a deferred-capable WAL")
	}
}
