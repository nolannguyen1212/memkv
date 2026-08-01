package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/engine"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
	"github.com/william1nguyen/valkeydb/internal/wal"
)

func command(name string, args ...string) (string, []resp.Value) {
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.Value{Type: resp.TypeBulkString, String: arg}
	}
	return name, values
}

func execute(conn *engine.ConnContext, name string, args ...string) resp.Value {
	cmd, values := command(name, args...)
	return engine.Execute(conn, cmd, values)
}

func newStore() *store.Store {
	return store.New(store.Config{})
}

func TestAOFReplaysListPopsAndSortedSetMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	aof, err := wal.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aof.Close() }()

	source := engine.NewConnContext(engine.NewContext(newStore(), aof, nil), nil)
	execute(source, "RPUSH", "list", "a", "b", "c")
	execute(source, "LPOP", "list")
	execute(source, "RPOP", "list")
	execute(source, "ZADD", "scores", "1", "alice", "2", "bob")
	execute(source, "ZREM", "scores", "bob")

	target := engine.NewConnContext(engine.NewContext(newStore(), nil, nil), nil)
	if err := aof.Load(path, func(name string, args []resp.Value) error {
		engine.Execute(target, name, args)
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
	aof, err := wal.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}

	keyspace := map[string]store.KeyMetadata{
		"scores": {Type: store.KeyTypeSortedSet},
	}
	if err := aof.RewriteAll(nil, nil, nil, nil,
		map[string]map[string]float64{"scores": {"alice": 1.5}}, keyspace, path); err != nil {
		t.Fatal(err)
	}
	set := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "SET"},
		{Type: resp.TypeBulkString, String: "after"},
		{Type: resp.TypeBulkString, String: "rewrite"},
	}}
	if err := aof.Append(set); err != nil {
		t.Fatal(err)
	}
	if err := aof.Close(); err != nil {
		t.Fatal(err)
	}

	loader, err := wal.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()
	target := engine.NewConnContext(engine.NewContext(newStore(), nil, nil), nil)
	if err := loader.Load(path, func(name string, args []resp.Value) error {
		engine.Execute(target, name, args)
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
	rdb, err := snapshot.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()

	data := snapshot.Data{
		KeyspaceData: map[string]store.KeyMetadata{
			"scores": {Type: store.KeyTypeSortedSet, Version: 3},
		},
		SortedSetData: map[string]map[string]float64{"scores": {"alice": 1.5}},
	}
	if err := rdb.Save(data, path); err != nil {
		t.Fatal(err)
	}
	loaded, err := rdb.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.KeyspaceData["scores"].Type != store.KeyTypeSortedSet {
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
	aof, err := wal.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aof.Close() }()
	if err := aof.Load(path, func(string, []resp.Value) error { return nil }); err == nil {
		t.Fatal("truncated AOF should return a recovery error")
	}
}

func TestRDBLoadRejectsTruncatedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.rdb")
	if err := os.WriteFile(path, []byte{0x7f, 0x01}, 0o600); err != nil {
		t.Fatal(err)
	}
	rdb, err := snapshot.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	if _, err := rdb.Load(path); err == nil {
		t.Fatal("truncated RDB should return a recovery error")
	}
}
