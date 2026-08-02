package engine

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/william1nguyen/memkv/internal/store"
)

var testStoreConfig = store.Config{
	ExpirationCheckInterval: time.Second,
}

type engineClock struct {
	now time.Time
}

func (clock *engineClock) Now() time.Time {
	return clock.now
}

func TestExpirationInvalidatesWatch(t *testing.T) {
	clock := &engineClock{now: time.Unix(100, 0)}
	base := NewContext(store.New(store.Config{Clock: clock}), nil, nil, SystemConfig{})
	watcher := newTestConn(base)
	reader := newTestConn(base)
	exec(watcher, "SET", "key", "value", "PXAT", "102000")
	exec(watcher, "WATCH", "key")
	clock.now = time.Unix(103, 0)
	exec(reader, "GET", "key")
	exec(watcher, "MULTI")
	result := exec(watcher, "EXEC")

	if result.Type != ResultArray {
		t.Fatalf("EXEC type = %v, want array", result.Type)
	}

	if result.Array != nil {
		t.Fatalf("EXEC array = %#v, want nil", result.Array)
	}

	if !result.IsNull {
		t.Fatalf("EXEC result = %#v, want null", result)
	}
}

func newTestBase() *Context {
	return NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{})
}

func newTestContext() *ConnContext {
	return NewConnContext(newTestBase(), nil)
}

func newTestConn(base *Context) *ConnContext {
	return NewConnContext(base, nil)
}

func exec(connContext *ConnContext, name string, args ...string) Result {
	if connContext.running.Load() {
		return Execute(connContext, name, args)
	}

	return BootstrapExecute(connContext, name, args)
}

func TestMultiExecBasic(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "MULTI")
	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "SET", "counter", "1")
	result := exec(connContext, "EXEC")

	if result.Type != ResultArray {
		t.Fatalf("EXEC expected array, got type %v", result.Type)
	}

	if len(result.Array) != 2 {
		t.Fatalf("EXEC expected 2 results, got %d", len(result.Array))
	}

	val, _ := connContext.Store.Dictionary.Get("foo")

	if val != "bar" {
		t.Errorf("expected foo=bar, got %q", val)
	}
}

func TestInvalidCommandIsRejectedBeforeQueue(t *testing.T) {
	connection := newTestContext()
	exec(connection, "MULTI")

	result := exec(connection, "SET", "only-key")

	if result.Type != ResultError {
		t.Fatalf("result = %#v, want an error", result)
	}

	if len(connection.Transaction.Queue) != 0 {
		t.Fatalf("queue = %v, want empty queue", connection.Transaction.Queue)
	}

	if !connection.Transaction.Dirty {
		t.Fatal("transaction should be dirty")
	}

	result = exec(connection, "EXEC")

	if result.Type != ResultError {
		t.Fatalf("EXEC result = %#v, want an error", result)
	}

	if !strings.HasPrefix(result.String, "EXECABORT") {
		t.Fatalf("EXEC error = %q, want EXECABORT", result.String)
	}
}

func TestExecCommitsOneWALBatch(t *testing.T) {
	appender := &recordingAppender{}
	conn := NewConnContext(NewContext(store.New(testStoreConfig), appender, nil, SystemConfig{}), nil)
	exec(conn, "MULTI")
	exec(conn, "SET", "first", "1")
	exec(conn, "SET", "second", "2")
	result := exec(conn, "EXEC")

	if result.Type != ResultArray || len(result.Array) != 2 {
		t.Fatalf("EXEC returned %#v", result)
	}

	appender.mutex.Lock()
	defer appender.mutex.Unlock()

	if appender.batches != 1 || len(appender.values) != 2 {
		t.Fatalf("WAL batches = %d, values = %d, want 1 batch with 2 values", appender.batches, len(appender.values))
	}
}

func TestExecWALFailureDoesNotChangeLiveStore(t *testing.T) {
	database := store.New(testStoreConfig)
	database.SetString("existing", "before", time.Time{})
	conn := NewConnContext(NewContext(database, failingAppender{}, nil, SystemConfig{}), nil)
	exec(conn, "MULTI")
	exec(conn, "SET", "existing", "after")
	exec(conn, "SET", "new", "value")

	if result := exec(conn, "EXEC"); result.Type != ResultError {
		t.Fatalf("EXEC returned %#v, want persistence error", result)
	}

	if value, exists := database.Dictionary.Get("existing"); !exists || value != "before" {
		t.Fatalf("existing value = %q, exists=%v, want before", value, exists)
	}

	if _, exists := database.Dictionary.Get("new"); exists {
		t.Fatal("failed EXEC created new key")
	}
}

func TestExecCommitsCapacityEvictionInOneWALBatch(t *testing.T) {
	maxKeys := 1
	database := store.New(store.Config{MaxKeys: &maxKeys})
	database.SetString("first", "one", time.Time{})
	appender := &recordingAppender{}
	conn := NewConnContext(NewContext(database, appender, nil, SystemConfig{}), nil)
	exec(conn, "MULTI")
	exec(conn, "SET", "second", "two")
	result := exec(conn, "EXEC")

	if result.Type != ResultArray || len(result.Array) != 1 {
		t.Fatalf("EXEC returned %#v", result)
	}

	if result.Array[0].Type != ResultSimpleString {
		t.Fatalf("SET result = %#v, want simple string", result.Array[0])
	}

	if appender.batches != 1 {
		t.Fatalf("WAL batches = %d, want 1", appender.batches)
	}

	if names := appender.commandNames(); !reflect.DeepEqual(names, []string{"SET", "DEL"}) {
		t.Fatalf("WAL commands = %v, want [SET DEL]", names)
	}

	if _, exists := database.Dictionary.Get("first"); exists {
		t.Fatal("eviction victim still exists")
	}

	if value, exists := database.Dictionary.Get("second"); !exists || value != "two" {
		t.Fatalf("second value = %q, exists = %v", value, exists)
	}
}

func TestMultiQueuesAllTypes(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "MULTI")
	exec(connContext, "SET", "str", "hello")
	exec(connContext, "SADD", "myset", "a", "b")
	exec(connContext, "LPUSH", "mylist", "x")
	exec(connContext, "HSET", "myhash", "field", "value")
	exec(connContext, "ZADD", "myzset", "1.0", "member")
	result := exec(connContext, "EXEC")

	if len(result.Array) != 5 {
		t.Fatalf("expected 5 results, got %d", len(result.Array))
	}

	v, _ := connContext.Store.Dictionary.Get("str")

	if v != "hello" {
		t.Error("SET inside MULTI did not execute")
	}

	members, _ := connContext.Store.Set.Members("myset")

	if len(members) != 2 {
		t.Error("SADD inside MULTI did not execute")
	}

	if connContext.Store.List.Length("mylist") != 1 {
		t.Error("LPUSH inside MULTI did not execute")
	}

	hv, _ := connContext.Store.Hash.Get("myhash", "field")

	if hv != "value" {
		t.Error("HSET inside MULTI did not execute")
	}

	_, ok := connContext.Store.SortedSet.Score("myzset", "member")

	if !ok {
		t.Error("ZADD inside MULTI did not execute")
	}
}

func TestNestedMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "MULTI")

	if result.Type != ResultError {
		t.Errorf("nested MULTI expected error, got %v", result.Type)
	}

	exec(connContext, "EXEC")

	if connContext.Transaction.Status != TransactionIdle {
		t.Error("expected Transaction to be idle after EXEC")
	}
}

func TestExecWithoutMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	result := exec(connContext, "EXEC")

	if result.Type != ResultError {
		t.Errorf("EXEC without MULTI expected error, got %v", result.Type)
	}
}

func TestCommandInsideMultiReturnsQueued(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "SET", "k", "v")

	if result.Type != ResultSimpleString || result.String != "QUEUED" {
		t.Errorf("expected QUEUED, got %v %q", result.Type, result.String)
	}

	exec(connContext, "EXEC")
}

func TestExecErrorInOneSlotDoesNotAbort(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "MULTI")
	exec(connContext, "SET", "k", "hello")
	exec(connContext, "ZADD", "k", "notfloat", "member")
	exec(connContext, "GET", "k")
	result := exec(connContext, "EXEC")

	if len(result.Array) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.Array))
	}

	if result.Array[0].Type != ResultSimpleString {
		t.Errorf("slot 0 (SET) should succeed, got type %v", result.Array[0].Type)
	}

	if result.Array[1].Type != ResultError {
		t.Errorf("slot 1 (ZADD bad float) should be error, got type %v", result.Array[1].Type)
	}

	if result.Array[2].String != "hello" {
		t.Errorf("slot 2 (GET) expected 'hello', got %q", result.Array[2].String)
	}
}

func TestDiscardClearsQueue(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "MULTI")
	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "DISCARD")

	if connContext.Transaction.Status != TransactionIdle {
		t.Error("expected TransactionIdle after DISCARD")
	}

	if len(connContext.Transaction.Queue) != 0 {
		t.Error("expected empty queue after DISCARD")
	}

	_, ok := connContext.Store.Dictionary.Get("foo")

	if ok {
		t.Error("SET should not have executed after DISCARD")
	}
}

func TestDiscardWithoutMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	result := exec(connContext, "DISCARD")

	if result.Type != ResultError {
		t.Errorf("DISCARD without MULTI expected error, got %v", result.Type)
	}
}

func TestWatchNoConflictExecSucceeds(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "SET", "counter", "0")

	exec(connContext, "WATCH", "counter")
	exec(connContext, "MULTI")
	exec(connContext, "SET", "counter", "1")
	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array == nil {
		t.Fatal("EXEC should succeed when watched key was not modified")
	}

	val, _ := connContext.Store.Dictionary.Get("counter")

	if val != "1" {
		t.Errorf("expected counter=1, got %q", val)
	}
}

func TestWatchConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "SET", "counter", "0")
	exec(connContext, "WATCH", "counter")
	exec(connContext, "MULTI")
	exec(connContext, "SET", "counter", "1")

	exec(other, "SET", "counter", "999")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should return nil array when watched key was modified")
	}

	val, _ := connContext.Store.Dictionary.Get("counter")

	if val != "999" {
		t.Errorf("expected counter=999 (from other conn), got %q", val)
	}
}

func TestWatchSetConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "SADD", "myset", "a")
	exec(connContext, "WATCH", "myset")
	exec(connContext, "MULTI")
	exec(connContext, "SADD", "myset", "b")

	exec(other, "SREM", "myset", "a")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched set key was modified via SREM")
	}
}

func TestWatchListConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "RPUSH", "mylist", "x")
	exec(connContext, "WATCH", "mylist")
	exec(connContext, "MULTI")
	exec(connContext, "RPUSH", "mylist", "y")

	exec(other, "LPOP", "mylist")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched list key was modified via LPOP")
	}
}

func TestWatchHashConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "HSET", "myhash", "field", "v1")
	exec(connContext, "WATCH", "myhash")
	exec(connContext, "MULTI")
	exec(connContext, "HSET", "myhash", "field", "v2")

	exec(other, "HDEL", "myhash", "field")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched hash key was modified via HDEL")
	}
}

func TestWatchZsetConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "ZADD", "myzset", "1.0", "a")
	exec(connContext, "WATCH", "myzset")
	exec(connContext, "MULTI")
	exec(connContext, "ZADD", "myzset", "2.0", "b")

	exec(other, "ZREM", "myzset", "a")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched zset key was modified via ZREM")
	}
}

func TestWatchDeleteConflictAborts(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "WATCH", "foo")
	exec(connContext, "MULTI")
	exec(connContext, "SET", "foo", "baz")

	exec(other, "DEL", "foo")

	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched key was deleted by another conn")
	}
}

func TestWatchInsideMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "WATCH", "foo")

	if result.Type != ResultError {
		t.Errorf("WATCH inside MULTI should return error, got type %v %q", result.Type, result.String)
	}

	exec(connContext, "DISCARD")
}

func TestUnwatchPreventsAbort(t *testing.T) {
	base := newTestBase()
	connContext := newTestConn(base)
	other := newTestConn(base)

	exec(connContext, "SET", "k", "0")
	exec(connContext, "WATCH", "k")
	exec(connContext, "UNWATCH")

	exec(other, "SET", "k", "999")

	exec(connContext, "MULTI")
	exec(connContext, "SET", "k", "1")
	result := exec(connContext, "EXEC")

	if result.Type != ResultArray || result.Array == nil {
		t.Fatal("EXEC should succeed after UNWATCH even if key was modified")
	}
}

func TestWatchRegistryUnwatchOnDisconnect(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "WATCH", "foo")

	connContext.Disconnect(connContext.ID)

	entries := connContext.watches.watchers["foo"]

	if len(entries) != 0 {
		t.Errorf("expected no watchers after disconnect, got %d", len(entries))
	}
}

func TestWatchRegistriesAreIsolatedByEngine(t *testing.T) {
	first := newTestContext()
	second := newTestContext()

	exec(first, "WATCH", "shared")
	second.Store.Keyspace.BumpVersion("shared")

	if first.watches.isDirty(first.ID) {
		t.Fatal("a mutation in another engine dirtied this engine's transaction")
	}
}

func TestStoreMutationInvalidatesWatch(t *testing.T) {
	connection := newTestContext()
	exec(connection, "SADD", "set", "first")
	exec(connection, "WATCH", "set")

	connection.Store.Set.Add("set", "second")

	if !connection.watches.isDirty(connection.ID) {
		t.Fatal("store mutation did not invalidate WATCH")
	}
}

func TestTxStateReset(t *testing.T) {
	connContext := newTestContext()

	connContext.Transaction.Status = TransactionQueuing
	connContext.Transaction.Enqueue("SET", []string{"k", "v"})
	connContext.Transaction.Dirty = true
	connContext.Transaction.Watches = map[string]uint64{"foo": 5}
	connContext.Transaction.Reset()

	if connContext.Transaction.Status != TransactionIdle {
		t.Error("Status should be TransactionIdle after Reset")
	}

	if len(connContext.Transaction.Queue) != 0 {
		t.Error("Queue should be empty after Reset")
	}

	if connContext.Transaction.Dirty {
		t.Error("Dirty should be false after Reset")
	}

	if connContext.Transaction.Watches != nil {
		t.Error("Watches should be nil after Reset")
	}
}

func TestConcurrentWritesAndWatch(t *testing.T) {
	ctx := NewContext(store.New(store.Config{
		ExpirationCheckInterval: time.Second,
	}), nil, nil, SystemConfig{})

	done := make(chan struct{})

	go func() {
		connContext := NewConnContext(ctx, nil)

		for range 1000 {
			exec(connContext, "SET", "shared", "value")
		}

		close(done)
	}()

	for range 100 {
		connContext := NewConnContext(ctx, nil)
		exec(connContext, "WATCH", "shared")
		exec(connContext, "MULTI")
		exec(connContext, "GET", "shared")
		exec(connContext, "EXEC")
	}

	<-done
}

func TestExecDoesNotAllowOtherCommandsToInterleave(t *testing.T) {
	base := newTestBase()
	transactionConn := newTestConn(base)
	otherConn := newTestConn(base)

	// Use per-test command names to make failures easier to identify.
	suffix := fmt.Sprintf("%d", transactionConn.ID)
	blockCommand := "TEST_TX_BLOCK_" + suffix
	otherCommand := "TEST_TX_OTHER_" + suffix
	transactionEntered := make(chan struct{})
	releaseTransaction := make(chan struct{})
	otherEntered := make(chan struct{})
	var transactionCalls atomic.Int32

	base.Registry.Register(blockCommand, func(_ *ConnContext, _ []string) Result {
		if transactionCalls.Add(1) == 1 {
			close(transactionEntered)
		}

		<-releaseTransaction
		return okReply()
	})
	base.Registry.Register(otherCommand, func(_ *ConnContext, _ []string) Result {
		close(otherEntered)
		return okReply()
	})

	exec(transactionConn, "MULTI")
	exec(transactionConn, blockCommand)
	execDone := make(chan struct{})
	go func() {
		exec(transactionConn, "EXEC")
		close(execDone)
	}()

	select {
	case <-transactionEntered:
	case <-time.After(time.Second):
		t.Fatal("transaction command did not start")
	}

	otherDone := make(chan struct{})
	go func() {
		exec(otherConn, otherCommand)
		close(otherDone)
	}()

	select {
	case <-otherEntered:
		t.Fatal("command from another connection interleaved with EXEC")
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseTransaction)

	select {
	case <-execDone:
	case <-time.After(time.Second):
		t.Fatal("EXEC did not finish")
	}

	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("blocked command did not resume after EXEC")
	}
}
