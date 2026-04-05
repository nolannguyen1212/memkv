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

func exec(cctx *ConnContext, name string, args ...string) protocol.Value {
	vals := make([]protocol.Value, len(args))
	for i, a := range args {
		vals[i] = protocol.Value{Type: protocol.TypeBulkString, String: a}
	}
	return Execute(cctx, name, vals)
}

func TestMultiExecBasic(t *testing.T) {
	cctx := newTestContext()

	exec(cctx, "MULTI")
	exec(cctx, "SET", "foo", "bar")
	exec(cctx, "SET", "counter", "1")
	result := exec(cctx, "EXEC")

	if result.Type != protocol.TypeArray {
		t.Fatalf("EXEC expected array, got type %v", result.Type)
	}
	if len(result.Array) != 2 {
		t.Fatalf("EXEC expected 2 results, got %d", len(result.Array))
	}

	val, _ := cctx.Store.Dictionary.Get("foo")
	if val != "bar" {
		t.Errorf("expected foo=bar, got %q", val)
	}
}

func TestMultiQueuesAllTypes(t *testing.T) {
	cctx := newTestContext()

	exec(cctx, "MULTI")
	exec(cctx, "SET", "str", "hello")
	exec(cctx, "SADD", "myset", "a", "b")
	exec(cctx, "LPUSH", "mylist", "x")
	exec(cctx, "HSET", "myhash", "field", "value")
	exec(cctx, "ZADD", "myzset", "1.0", "member")
	result := exec(cctx, "EXEC")

	if len(result.Array) != 5 {
		t.Fatalf("expected 5 results, got %d", len(result.Array))
	}

	v, _ := cctx.Store.Dictionary.Get("str")
	if v != "hello" {
		t.Error("SET inside MULTI did not execute")
	}
	members, _ := cctx.Store.Set.Members("myset")
	if len(members) != 2 {
		t.Error("SADD inside MULTI did not execute")
	}
	if cctx.Store.List.Length("mylist") != 1 {
		t.Error("LPUSH inside MULTI did not execute")
	}
	hv, _ := cctx.Store.HashMap.Get("myhash", "field")
	if hv != "value" {
		t.Error("HSET inside MULTI did not execute")
	}
	_, ok := cctx.Store.SortedList.Score("myzset", "member")
	if !ok {
		t.Error("ZADD inside MULTI did not execute")
	}
}

func TestNestedMultiReturnsError(t *testing.T) {
	cctx := newTestContext()
	exec(cctx, "MULTI")
	result := exec(cctx, "MULTI")

	if result.Type != protocol.TypeError {
		t.Errorf("nested MULTI expected error, got %v", result.Type)
	}
	exec(cctx, "EXEC")
	if cctx.TX.Status != TxIdle {
		t.Error("expected TX to be idle after EXEC")
	}
}

func TestExecWithoutMultiReturnsError(t *testing.T) {
	cctx := newTestContext()
	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeError {
		t.Errorf("EXEC without MULTI expected error, got %v", result.Type)
	}
}

func TestCommandInsideMultiReturnsQueued(t *testing.T) {
	cctx := newTestContext()
	exec(cctx, "MULTI")
	result := exec(cctx, "SET", "k", "v")

	if result.Type != protocol.TypeSimpleString || result.String != "QUEUED" {
		t.Errorf("expected QUEUED, got %v %q", result.Type, result.String)
	}

	exec(cctx, "EXEC")
}

func TestExecErrorInOneSlotDoesNotAbort(t *testing.T) {
	cctx := newTestContext()

	exec(cctx, "MULTI")
	exec(cctx, "SET", "k", "hello")
	exec(cctx, "ZADD", "k", "notfloat", "member")
	exec(cctx, "GET", "k")
	result := exec(cctx, "EXEC")

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
	cctx := newTestContext()

	exec(cctx, "MULTI")
	exec(cctx, "SET", "foo", "bar")
	exec(cctx, "DISCARD")

	if cctx.TX.Status != TxIdle {
		t.Error("expected TxIdle after DISCARD")
	}
	if len(cctx.TX.Queue) != 0 {
		t.Error("expected empty queue after DISCARD")
	}
	_, ok := cctx.Store.Dictionary.Get("foo")
	if ok {
		t.Error("SET should not have executed after DISCARD")
	}
}

func TestDiscardWithoutMultiReturnsError(t *testing.T) {
	cctx := newTestContext()
	result := exec(cctx, "DISCARD")
	if result.Type != protocol.TypeError {
		t.Errorf("DISCARD without MULTI expected error, got %v", result.Type)
	}
}

func TestWatchNoConflictExecSucceeds(t *testing.T) {
	cctx := newTestContext()
	exec(cctx, "SET", "counter", "0")

	exec(cctx, "WATCH", "counter")
	exec(cctx, "MULTI")
	exec(cctx, "SET", "counter", "1")
	result := exec(cctx, "EXEC")

	if result.Type != protocol.TypeArray || result.Array == nil {
		t.Fatal("EXEC should succeed when watched key was not modified")
	}
	val, _ := cctx.Store.Dictionary.Get("counter")
	if val != "1" {
		t.Errorf("expected counter=1, got %q", val)
	}
}

func TestWatchConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "SET", "counter", "0")
	exec(cctx, "WATCH", "counter")
	exec(cctx, "MULTI")
	exec(cctx, "SET", "counter", "1")

	exec(other, "SET", "counter", "999")

	result := exec(cctx, "EXEC")

	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should return nil array when watched key was modified")
	}
	val, _ := cctx.Store.Dictionary.Get("counter")
	if val != "999" {
		t.Errorf("expected counter=999 (from other conn), got %q", val)
	}
}

func TestWatchSetConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "SADD", "myset", "a")
	exec(cctx, "WATCH", "myset")
	exec(cctx, "MULTI")
	exec(cctx, "SADD", "myset", "b")

	exec(other, "SREM", "myset", "a")

	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched set key was modified via SREM")
	}
}

func TestWatchListConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "RPUSH", "mylist", "x")
	exec(cctx, "WATCH", "mylist")
	exec(cctx, "MULTI")
	exec(cctx, "RPUSH", "mylist", "y")

	exec(other, "LPOP", "mylist")

	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched list key was modified via LPOP")
	}
}

func TestWatchHashConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "HSET", "myhash", "field", "v1")
	exec(cctx, "WATCH", "myhash")
	exec(cctx, "MULTI")
	exec(cctx, "HSET", "myhash", "field", "v2")

	exec(other, "HDEL", "myhash", "field")

	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched hash key was modified via HDEL")
	}
}

func TestWatchZsetConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "ZADD", "myzset", "1.0", "a")
	exec(cctx, "WATCH", "myzset")
	exec(cctx, "MULTI")
	exec(cctx, "ZADD", "myzset", "2.0", "b")

	exec(other, "ZREM", "myzset", "a")

	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched zset key was modified via ZREM")
	}
}

func TestWatchDeleteConflictAborts(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "SET", "foo", "bar")
	exec(cctx, "WATCH", "foo")
	exec(cctx, "MULTI")
	exec(cctx, "SET", "foo", "baz")

	exec(other, "DEL", "foo")

	result := exec(cctx, "EXEC")
	if result.Type != protocol.TypeArray || result.Array != nil {
		t.Fatal("EXEC should abort when watched key was deleted by another conn")
	}
}

func TestWatchInsideMultiReturnsError(t *testing.T) {
	cctx := newTestContext()
	exec(cctx, "MULTI")
	result := exec(cctx, "WATCH", "foo")
	if result.Type != protocol.TypeError {
		t.Errorf("WATCH inside MULTI should return error, got type %v %q", result.Type, result.String)
	}
	exec(cctx, "DISCARD")
}

func TestUnwatchPreventsAbort(t *testing.T) {
	base := newTestBase()
	cctx := newTestConn(base)
	other := newTestConn(base)

	exec(cctx, "SET", "k", "0")
	exec(cctx, "WATCH", "k")
	exec(cctx, "UNWATCH")

	exec(other, "SET", "k", "999")

	exec(cctx, "MULTI")
	exec(cctx, "SET", "k", "1")
	result := exec(cctx, "EXEC")

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
	d := store.Dictionary

	if d.Version("missing") != 0 {
		t.Error("missing key should have version 0")
	}

	d.Set("k", "v1", 0)
	v1 := d.Version("k")

	d.Set("k", "v2", 0)
	v2 := d.Version("k")

	if v2 <= v1 {
		t.Errorf("version should increase on write: v1=%d v2=%d", v1, v2)
	}
}

func TestWatchRegistryUnwatchOnDisconnect(t *testing.T) {
	cctx := newTestContext()

	exec(cctx, "SET", "foo", "bar")
	exec(cctx, "WATCH", "foo")

	UnwatchConn(cctx.ID)

	globalWatch.mu.Lock()
	entries := globalWatch.watchers["foo"]
	globalWatch.mu.Unlock()

	if len(entries) != 0 {
		t.Errorf("expected no watchers after UnwatchConn, got %d", len(entries))
	}
}

func TestTxStateReset(t *testing.T) {
	cctx := newTestContext()

	cctx.TX.Status = TxQueueing
	cctx.TX.Enqueue("SET", []protocol.Value{{String: "k"}, {String: "v"}})
	cctx.TX.Dirty = true
	cctx.TX.Watches = map[string]uint64{"foo": 5}

	cctx.TX.Reset()

	if cctx.TX.Status != TxIdle {
		t.Error("Status should be TxIdle after Reset")
	}
	if len(cctx.TX.Queue) != 0 {
		t.Error("Queue should be empty after Reset")
	}
	if cctx.TX.Dirty {
		t.Error("Dirty should be false after Reset")
	}
	if cctx.TX.Watches != nil {
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
		c := NewConnContext(ctx)
		for i := 0; i < 1000; i++ {
			exec(c, "SET", "shared", "value")
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		c := NewConnContext(ctx)
		exec(c, "WATCH", "shared")
		exec(c, "MULTI")
		exec(c, "GET", "shared")
		exec(c, "EXEC")
	}

	<-done
}

func TestDatastructures(t *testing.T) {
	_ = datastructure.DefaultExpirationConfig
}
