package engine

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/mutation"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type recordingAppender struct {
	mutex   sync.Mutex
	values  mutation.Batch
	batches int
}

func (appender *recordingAppender) AppendBatch(values mutation.Batch) error {
	appender.mutex.Lock()
	defer appender.mutex.Unlock()
	appender.batches++
	appender.values = append(appender.values, values...)
	return nil
}

type failingAppender struct{}
type failAtAppender struct {
	call int
	at   int
}

func (failingAppender) Append(mutation.Command) error { return errors.New("disk full") }
func (appender *failAtAppender) Append(mutation.Command) error {
	appender.call++

	if appender.call == appender.at {
		return errors.New("disk full")
	}

	return nil
}

func (appender *recordingAppender) Append(value mutation.Command) error {
	appender.mutex.Lock()
	defer appender.mutex.Unlock()
	appender.values = append(appender.values, value)
	return nil
}

func (appender *recordingAppender) commandNames() []string {
	appender.mutex.Lock()
	defer appender.mutex.Unlock()
	result := make([]string, 0, len(appender.values))

	for _, value := range appender.values {
		result = append(result, value.Name)
	}

	return result
}

func TestTypedKeyspaceRejectsWrongTypeAndSetReplacesType(t *testing.T) {
	conn := newTestContext()

	if result := exec(conn, "SADD", "shared", "member"); result.Type != ResultInteger || result.Number != 1 {
		t.Fatalf("SADD failed: %#v", result)
	}

	if result := exec(conn, "GET", "shared"); result.Type != ResultError {
		t.Fatalf("GET against set should return WRONGTYPE, got %#v", result)
	}

	if result := exec(conn, "LPUSH", "shared", "item"); result.Type != ResultError {
		t.Fatalf("LPUSH against set should return WRONGTYPE, got %#v", result)
	}

	if result := exec(conn, "SET", "shared", "value"); result.Type != ResultSimpleString {
		t.Fatalf("SET should replace the old type, got %#v", result)
	}

	if result := exec(conn, "SCARD", "shared"); result.Type != ResultError {
		t.Fatalf("old set type should no longer exist, got %#v", result)
	}

	if value, exists := conn.Store.Dictionary.Get("shared"); !exists || value != "value" {
		t.Fatalf("replacement string missing: value=%q exists=%v", value, exists)
	}

	if conn.Store.Set.Cardinality("shared") != 0 {
		t.Fatal("SET left stale set payload behind")
	}

	if result := exec(conn, "DEL", "shared", "shared"); result.Number != 1 {
		t.Fatalf("DEL should count a typed key once, got %#v", result)
	}
}

func TestListPopAndSortedSetMutationsUseDurabilityPipeline(t *testing.T) {
	appender := &recordingAppender{}
	conn := NewConnContext(NewContext(store.New(testStoreConfig), appender, nil, SystemConfig{}), nil)

	exec(conn, "RPUSH", "list", "a", "b")
	exec(conn, "LPOP", "list")
	exec(conn, "RPOP", "list")
	exec(conn, "ZADD", "scores", "1.5", "alice", "2", "bob")
	exec(conn, "ZREM", "scores", "bob")

	want := []string{"RPUSH", "LPOP", "RPOP", "ZADD", "ZREM"}
	got := appender.commandNames()

	if len(got) != len(want) {
		t.Fatalf("durability commands = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("durability commands = %v, want %v", got, want)
		}
	}
}

func TestReplicationSnapshotIncludesSortedSets(t *testing.T) {
	source := newTestContext()
	exec(source, "ZADD", "scores", "1.5", "alice", "2", "bob")

	target := newTestContext()

	if err := target.RestoreSnapshot(source.ReplicationSnapshot()); err != nil {
		t.Fatal(err)
	}

	if score, exists := target.Store.SortedSet.Score("scores", "alice"); !exists || score != 1.5 {
		t.Fatalf("sorted set missing after snapshot replay: score=%v exists=%v", score, exists)
	}

	if score, exists := target.Store.SortedSet.Score("scores", "bob"); !exists || score != 2 {
		t.Fatalf("sorted set missing after snapshot replay: score=%v exists=%v", score, exists)
	}
}

func TestPersistenceFailureStopsFurtherWrites(t *testing.T) {
	base := NewContext(store.New(testStoreConfig), failingAppender{}, nil, SystemConfig{})
	conn := NewConnContext(base, nil)

	if result := exec(conn, "SET", "first", "value"); result.Type != ResultError {
		t.Fatalf("failed WAL append should fail the command, got %#v", result)
	}

	if _, exists := conn.Store.Dictionary.Get("first"); exists {
		t.Fatal("failed WAL append changed memory")
	}

	if result := exec(conn, "SET", "second", "value"); result.Type != ResultError || result.String == "" {
		t.Fatalf("writes should remain disabled after persistence failure, got %#v", result)
	}

	if _, exists := conn.Store.Dictionary.Get("second"); exists {
		t.Fatal("write executed after persistence entered fail-stop mode")
	}
}

func TestSetCommitsAbsoluteExpirationAsOneRecord(t *testing.T) {
	appender := &recordingAppender{}
	conn := NewConnContext(NewContext(store.New(testStoreConfig), appender, nil, SystemConfig{}), nil)

	if result := exec(conn, "SET", "key", "value", "PX", "5000"); result.Type != ResultSimpleString {
		t.Fatalf("SET returned %#v", result)
	}

	appender.mutex.Lock()
	defer appender.mutex.Unlock()

	if len(appender.values) != 1 {
		t.Fatalf("WAL records = %d, want 1", len(appender.values))
	}

	record := appender.values[0]

	if len(record.Args) != 4 || record.Args[2] != "PXAT" {
		t.Fatalf("WAL record = %#v, want SET key value PXAT timestamp", record)
	}

	if conn.Store.Keyspace.Version("key") != 1 {
		t.Fatalf("version = %d, want 1", conn.Store.Keyspace.Version("key"))
	}
}

func TestFailedWALPreservesStateForDeleteAndExpire(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
	}{
		{name: "delete", command: "DEL", args: []string{"key"}},
		{name: "expire", command: "EXPIRE", args: []string{"key", "10"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := store.New(testStoreConfig)
			database.SetString("key", "value", time.Time{})
			conn := NewConnContext(NewContext(database, failingAppender{}, nil, SystemConfig{}), nil)

			if result := exec(conn, test.command, test.args...); result.Type != ResultError {
				t.Fatalf("%s returned %#v, want persistence error", test.command, result)
			}

			if value, exists := database.Dictionary.Get("key"); !exists || value != "value" {
				t.Fatalf("failed %s changed value: value=%q exists=%v", test.command, value, exists)
			}

			if ttl := database.Keyspace.TTL("key"); ttl != store.TTLNoExpire {
				t.Fatalf("failed %s changed TTL to %d", test.command, ttl)
			}
		})
	}
}

func TestFailedWALPreservesCollectionState(t *testing.T) {
	tests := []struct {
		name    string
		state   store.LogicalSnapshot
		command string
		args    []string
	}{
		{
			name: "set add",
			state: store.LogicalSnapshot{
				Keyspace: map[string]store.KeyMetadata{"key": {Type: store.KeyTypeSet}},
				Sets:     map[string]store.SetEntry{"key": {Members: map[string]struct{}{"old": {}}}},
			},
			command: "SADD",
			args:    []string{"key", "new"},
		},
		{
			name: "hash delete",
			state: store.LogicalSnapshot{
				Keyspace: map[string]store.KeyMetadata{"key": {Type: store.KeyTypeHash}},
				Hashes:   map[string]map[string]string{"key": {"field": "value"}},
			},
			command: "HDEL",
			args:    []string{"key", "field"},
		},
		{
			name: "list pop",
			state: store.LogicalSnapshot{
				Keyspace: map[string]store.KeyMetadata{"key": {Type: store.KeyTypeList}},
				Lists:    map[string][]string{"key": {"a", "b"}},
			},
			command: "LPOP",
			args:    []string{"key"},
		},
		{
			name: "sorted set update",
			state: store.LogicalSnapshot{
				Keyspace:   map[string]store.KeyMetadata{"key": {Type: store.KeyTypeSortedSet}},
				SortedSets: map[string]map[string]float64{"key": {"member": 1}},
			},
			command: "ZADD",
			args:    []string{"key", "2", "member"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := store.New(testStoreConfig)

			if err := database.Restore(test.state, time.Now()); err != nil {
				t.Fatal(err)
			}

			before := database.Snapshot()
			conn := NewConnContext(NewContext(database, failingAppender{}, nil, SystemConfig{}), nil)

			if result := exec(conn, test.command, test.args...); result.Type != ResultError {
				t.Fatalf("%s returned %#v, want persistence error", test.command, result)
			}

			after := database.Snapshot()

			if !reflect.DeepEqual(after, before) {
				t.Fatalf("failed %s changed state\nbefore=%#v\nafter=%#v", test.command, before, after)
			}
		})
	}
}

func TestFailedAtomicWriteAndCapacityDeleteDoesNotChangeState(t *testing.T) {
	limit := 1
	database := store.New(store.Config{MaxKeys: &limit})
	database.SetString("first", "value", time.Time{})
	appender := &failAtAppender{at: 2}
	base := NewContext(database, appender, nil, SystemConfig{})
	watcher := NewConnContext(base, nil)
	conn := NewConnContext(base, nil)
	exec(watcher, "WATCH", "first")

	if result := exec(conn, "SET", "second", "value"); result.Type != ResultError {
		t.Fatalf("SET returned %#v, want persistence error", result)
	}

	if _, exists := database.Dictionary.Get("first"); !exists {
		t.Fatal("eviction deleted victim before WAL commit")
	}

	if _, exists := database.Dictionary.Get("second"); exists {
		t.Fatal("failed atomic batch applied the primary mutation")
	}

	if database.Capacity.Count() != 1 {
		t.Fatalf("FIFO key count = %d, want 1", database.Capacity.Count())
	}

	if base.watches.isDirty(watcher.ID) {
		t.Fatal("failed eviction invalidated WATCH before deleting victim")
	}
}

func TestWriteAndCapacityDeleteCommitAsOneBatch(t *testing.T) {
	limit := 1
	database := store.New(store.Config{MaxKeys: &limit})
	database.SetString("first", "value", time.Time{})
	appender := &recordingAppender{}
	conn := NewConnContext(NewContext(database, appender, nil, SystemConfig{}), nil)

	if result := exec(conn, "SET", "second", "value"); result.Type != ResultSimpleString {
		t.Fatalf("SET returned %#v", result)
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

	if value, exists := database.Dictionary.Get("second"); !exists || value != "value" {
		t.Fatalf("new value = %q, exists = %v", value, exists)
	}
}

func TestCommittedBatchDoesNotDeriveAnotherEviction(t *testing.T) {
	maxKeys := 2
	database := store.New(store.Config{MaxKeys: &maxKeys})
	database.SetString("a", "one", time.Time{})
	database.SetString("b", "two", time.Time{})
	state := database.Snapshot()
	state.FIFO = []string{"b", "a"}

	if err := database.Restore(state, database.Now()); err != nil {
		t.Fatal(err)
	}

	appender := &recordingAppender{}
	base := NewContext(database, appender, nil, SystemConfig{})
	batch := mutation.Batch{
		mutation.New("SET", "c", "three"),
		mutation.New("DEL", "a"),
	}

	if err := base.ApplyBatch(batch); err != nil {
		t.Fatal(err)
	}

	if names := appender.commandNames(); !reflect.DeepEqual(names, []string{"SET", "DEL"}) {
		t.Fatalf("committed commands = %v, want [SET DEL]", names)
	}

	if _, exists := database.Dictionary.Get("a"); exists {
		t.Fatal("canonical eviction victim still exists")
	}

	if _, exists := database.Dictionary.Get("b"); !exists {
		t.Fatal("batch derived and deleted an additional victim")
	}

	if _, exists := database.Dictionary.Get("c"); !exists {
		t.Fatal("canonical write was not applied")
	}
}

func TestSystemConfigIsIsolatedByEngine(t *testing.T) {
	first := NewConnContext(NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{Auth: "first"}), nil)
	second := NewConnContext(NewContext(store.New(testStoreConfig), nil, nil, SystemConfig{Auth: "second"}), nil)

	if result := exec(first, "AUTH", "first"); result.Type == ResultError {
		t.Fatalf("first engine rejected its password: %#v", result)
	}

	if result := exec(second, "AUTH", "first"); result.Type != ResultError {
		t.Fatalf("second engine accepted another engine's password: %#v", result)
	}
}

func TestDeferredCommandsAreNotRegistered(t *testing.T) {
	registry := NewRegistry()
	commands := []string{"BGSAVE", "MONITOR", "PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "SORT", "SEXPIRE", "STTL"}

	for _, command := range commands {
		if _, exists := registry.Lookup(command); exists {
			t.Errorf("%s should not be registered", command)
		}
	}
}

func TestMetricsAndConnectionIDsAreIsolatedByEngine(t *testing.T) {
	first := newTestBase()
	second := newTestBase()

	first.ConnectionOpened()
	first.CommandProcessed()
	defer first.ConnectionClosed()

	if first.metrics.totalCommands.Load() != 1 || second.metrics.totalCommands.Load() != 0 {
		t.Fatal("metrics leaked between engine instances")
	}

	if NewConnContext(first, nil).ID != 1 || NewConnContext(second, nil).ID != 1 {
		t.Fatal("connection IDs leaked between engine instances")
	}
}

func TestExpireRemovesMetadataAndPayloadForEveryKeyType(t *testing.T) {
	connection := newTestContext()
	setups := []struct {
		key     string
		command string
		args    []string
		empty   func() bool
	}{
		{"string", "SET", []string{"string", "value"}, func() bool {
			_, exists := connection.Store.Dictionary.Get("string")
			return !exists
		}},
		{"set", "SADD", []string{"set", "member"}, func() bool {
			return connection.Store.Set.Cardinality("set") == 0
		}},
		{"list", "RPUSH", []string{"list", "value"}, func() bool {
			return connection.Store.List.Length("list") == 0
		}},
		{"hash", "HSET", []string{"hash", "field", "value"}, func() bool {
			return connection.Store.Hash.FieldCount("hash") == 0
		}},
		{"zset", "ZADD", []string{"zset", "1", "member"}, func() bool {
			return connection.Store.SortedSet.Cardinality("zset") == 0
		}},
	}

	for _, setup := range setups {
		exec(connection, setup.command, setup.args...)

		if result := exec(connection, "EXPIRE", setup.key, "0"); result.Number != 1 {
			t.Fatalf("EXPIRE %s returned %#v", setup.key, result)
		}

		if _, exists := connection.Store.Keyspace.Type(setup.key); exists {
			t.Fatalf("%s metadata still exists", setup.key)
		}

		if !setup.empty() {
			t.Fatalf("%s payload still exists", setup.key)
		}
	}
}
