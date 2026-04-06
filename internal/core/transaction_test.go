package core

import (
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

var testStoreConfig = StoreConfig{
	ExpirationCheckInterval: time.Second,
	ExpirationMaxSampleSize: 20,
	ExpirationMaxRounds:     3,
}

func newTestBase() *Context {
	return NewContext(NewStore(testStoreConfig), nil)
}

func newTestContext() *ConnContext {
	return NewConnContext(newTestBase())
}

func newTestConn(base *Context) *ConnContext {
	return NewConnContext(base)
}

func exec(connContext *ConnContext, name string, args ...string) protocol.Value {
	vals := make([]protocol.Value, len(args))
	for i, arg := range args {
		vals[i] = protocol.Value{Type: protocol.TypeBulkString, String: arg}
	}
	return Execute(connContext, name, vals)
}

func TestMultiExecBasic(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "MULTI")
	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "SET", "counter", "1")
	result := exec(connContext, "EXEC")

	if result.Type != protocol.TypeArray {
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
	hv, _ := connContext.Store.HashMap.Get("myhash", "field")
	if hv != "value" {
		t.Error("HSET inside MULTI did not execute")
	}
	_, ok := connContext.Store.SortedList.Score("myzset", "member")
	if !ok {
		t.Error("ZADD inside MULTI did not execute")
	}
}

func TestNestedMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "MULTI")

	if result.Type != protocol.TypeError {
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
	if result.Type != protocol.TypeError {
		t.Errorf("EXEC without MULTI expected error, got %v", result.Type)
	}
}

func TestCommandInsideMultiReturnsQueued(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "SET", "k", "v")

	if result.Type != protocol.TypeSimpleString || result.String != "QUEUED" {
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
	if result.Array[0].Type != protocol.TypeSimpleString {
		t.Errorf("slot 0 (SET) should succeed, got type %v", result.Array[0].Type)
	}
	if result.Array[1].Type != protocol.TypeError {
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
	if result.Type != protocol.TypeError {
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

	if result.Type != protocol.TypeArray || result.Array == nil {
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

	if result.Type != protocol.TypeArray || result.Array != nil {
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
	if result.Type != protocol.TypeArray || result.Array != nil {
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
	if result.Type != protocol.TypeArray || result.Array != nil {
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
	if result.Type != protocol.TypeArray || result.Array != nil {
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
	if result.Type != protocol.TypeArray || result.Array != nil {
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
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched key was deleted by another conn")
	}
}

func TestWatchInsideMultiReturnsError(t *testing.T) {
	connContext := newTestContext()
	exec(connContext, "MULTI")
	result := exec(connContext, "WATCH", "foo")
	if result.Type != protocol.TypeError {
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

	if result.Type != protocol.TypeArray || result.Array == nil {
		t.Fatal("EXEC should succeed after UNWATCH even if key was modified")
	}
}

func TestDictionaryVersionBumpsOnWrite(t *testing.T) {
	store := NewStore(StoreConfig{
		ExpirationCheckInterval: time.Second,
		ExpirationMaxSampleSize: 20,
		ExpirationMaxRounds:     3,
	})
	dictionary := store.Dictionary

	if dictionary.Version("missing") != 0 {
		t.Error("missing key should have version 0")
	}

	dictionary.Set("k", "v1", 0)
	v1 := dictionary.Version("k")

	dictionary.Set("k", "v2", 0)
	v2 := dictionary.Version("k")

	if v2 <= v1 {
		t.Errorf("version should increase on write: v1=%d v2=%d", v1, v2)
	}
}

func TestWatchRegistryUnwatchOnDisconnect(t *testing.T) {
	connContext := newTestContext()

	exec(connContext, "SET", "foo", "bar")
	exec(connContext, "WATCH", "foo")

	UnwatchConn(connContext.ID)

	globalWatch.mutex.Lock()
	entries := globalWatch.watchers["foo"]
	globalWatch.mutex.Unlock()

	if len(entries) != 0 {
		t.Errorf("expected no watchers after UnwatchConn, got %d", len(entries))
	}
}

func TestTxStateReset(t *testing.T) {
	connContext := newTestContext()

	connContext.Transaction.Status = TransactionQueuing
	connContext.Transaction.Enqueue("SET", []protocol.Value{{String: "k"}, {String: "v"}})
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
	ctx := NewContext(NewStore(StoreConfig{
		ExpirationCheckInterval: time.Second,
		ExpirationMaxSampleSize: 20,
		ExpirationMaxRounds:     3,
	}), nil)

	done := make(chan struct{})

	go func() {
		connContext := NewConnContext(ctx)
		for i := 0; i < 1000; i++ {
			exec(connContext, "SET", "shared", "value")
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		connContext := NewConnContext(ctx)
		exec(connContext, "WATCH", "shared")
		exec(connContext, "MULTI")
		exec(connContext, "GET", "shared")
		exec(connContext, "EXEC")
	}

	<-done
}

func TestDatastructures(t *testing.T) {
	_ = datastructure.DefaultExpirationConfig
}
