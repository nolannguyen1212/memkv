package persistence_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/core"
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func command(name string, args ...string) (string, []protocol.Value) {
	values := make([]protocol.Value, len(args))
	for i, arg := range args {
		values[i] = protocol.Value{Type: protocol.TypeBulkString, String: arg}
	}
	return name, values
}

func execute(conn *core.ConnContext, name string, args ...string) protocol.Value {
	cmd, values := command(name, args...)
	return core.Execute(conn, cmd, values)
}

func newStore() *core.Store {
	return core.NewStore(core.StoreConfig{})
}

func TestAOFReplaysListPopsAndSortedSetMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	aof, err := persistence.OpenAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aof.Close() }()

	source := core.NewConnContext(core.NewContext(newStore(), aof, nil), nil)
	execute(source, "RPUSH", "list", "a", "b", "c")
	execute(source, "LPOP", "list")
	execute(source, "RPOP", "list")
	execute(source, "ZADD", "scores", "1", "alice", "2", "bob")
	execute(source, "ZREM", "scores", "bob")

	target := core.NewConnContext(core.NewContext(newStore(), nil, nil), nil)
	if err := aof.Load(path, func(name string, args []protocol.Value) error {
		core.Execute(target, name, args)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	values, exists := target.Store.List.Range("list", 0, -1)
	if !exists || len(values) != 1 || values[0] != "b" {
		t.Fatalf("replayed list = %v, exists=%v", values, exists)
	}
	if _, exists := target.Store.SortedSet.Score("scores", "alice"); !exists {
		t.Fatal("replayed sorted set lost alice")
	}
	if _, exists := target.Store.SortedSet.Score("scores", "bob"); exists {
		t.Fatal("replayed ZREM did not remove bob")
	}
}

func TestAOFRewriteKeepsSortedSetsAndContinuesAppending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	aof, err := persistence.OpenAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}

	keyspace := map[string]datastructure.KeyMetadata{
		"scores": {Type: datastructure.KeyTypeSortedSet},
	}
	if err := aof.RewriteAll(nil, nil, nil, nil,
		map[string]map[string]float64{"scores": {"alice": 1.5}}, keyspace, path); err != nil {
		t.Fatal(err)
	}
	set := protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, String: "SET"},
		{Type: protocol.TypeBulkString, String: "after"},
		{Type: protocol.TypeBulkString, String: "rewrite"},
	}}
	if err := aof.Append(set); err != nil {
		t.Fatal(err)
	}
	if err := aof.Close(); err != nil {
		t.Fatal(err)
	}

	loader, err := persistence.OpenAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()
	target := core.NewConnContext(core.NewContext(newStore(), nil, nil), nil)
	if err := loader.Load(path, func(name string, args []protocol.Value) error {
		core.Execute(target, name, args)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if score, exists := target.Store.SortedSet.Score("scores", "alice"); !exists || score != 1.5 {
		t.Fatalf("rewritten zset missing: score=%v exists=%v", score, exists)
	}
	if value, exists := target.Store.Dictionary.Get("after"); !exists || value != "rewrite" {
		t.Fatalf("post-rewrite append missing: value=%q exists=%v", value, exists)
	}
}

func TestRDBRoundTripIncludesTypedSortedSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	rdb, err := persistence.OpenRDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()

	snapshot := persistence.Snapshot{
		KeyspaceData: map[string]datastructure.KeyMetadata{
			"scores": {Type: datastructure.KeyTypeSortedSet, Version: 3},
		},
		SortedSetData: map[string]map[string]float64{"scores": {"alice": 1.5}},
	}
	if err := rdb.Save(snapshot, path); err != nil {
		t.Fatal(err)
	}
	loaded, err := rdb.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.KeyspaceData["scores"].Type != datastructure.KeyTypeSortedSet {
		t.Fatalf("key type not preserved: %#v", loaded.KeyspaceData["scores"])
	}
	if loaded.SortedSetData["scores"]["alice"] != 1.5 {
		t.Fatalf("sorted set not preserved: %#v", loaded.SortedSetData)
	}
}

func TestAOFLoadRejectsTruncatedCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.aof")
	if err := os.WriteFile(path, []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nval"), 0o600); err != nil {
		t.Fatal(err)
	}
	aof, err := persistence.OpenAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aof.Close() }()
	if err := aof.Load(path, func(string, []protocol.Value) error { return nil }); err == nil {
		t.Fatal("truncated AOF should return a recovery error")
	}
}

func TestRDBLoadRejectsTruncatedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.rdb")
	if err := os.WriteFile(path, []byte{0x7f, 0x01}, 0o600); err != nil {
		t.Fatal(err)
	}
	rdb, err := persistence.OpenRDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	if _, err := rdb.Load(path); err == nil {
		t.Fatal("truncated RDB should return a recovery error")
	}
}
