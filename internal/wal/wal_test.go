package wal_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/engine"
	"github.com/william1nguyen/valkeydb/internal/mutation"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
	wallog "github.com/william1nguyen/valkeydb/internal/wal"
)

func walCommand(value resp.Value) mutation.Command {
	args := make([]string, len(value.Array)-1)
	for index, argument := range value.Array[1:] {
		args[index] = argument.String
	}
	return mutation.New(value.Array[0].String, args...)
}

func walBatch(values []resp.Value) mutation.Batch {
	batch := make(mutation.Batch, len(values))
	for index, value := range values {
		batch[index] = walCommand(value)
	}
	return batch
}

func execute(conn *engine.ConnContext, name string, args ...string) engine.Result {
	return engine.Execute(conn, name, args)
}

func newStore() *store.Store {
	return store.New(store.Config{})
}

func TestWALReplaysListPopsAndSortedSetMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	wal, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	source := engine.NewConnContext(engine.NewContext(newStore(), wal, nil, engine.SystemConfig{}), nil)
	execute(source, "RPUSH", "list", "a", "b", "c")
	execute(source, "LPOP", "list")
	execute(source, "RPOP", "list")
	execute(source, "ZADD", "scores", "1", "alice", "2", "bob")
	execute(source, "ZREM", "scores", "bob")

	target := engine.NewConnContext(engine.NewContext(newStore(), nil, nil, engine.SystemConfig{}), nil)
	if err := wal.Load(path, func(command mutation.Command) error {
		engine.ExecuteCommand(target, command)
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

func TestWALRewriteKeepsSortedSetsAndContinuesAppending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	wal, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}

	state := store.LogicalSnapshot{
		Keyspace:   map[string]store.KeyMetadata{"scores": {Type: store.KeyTypeSortedSet}},
		SortedSets: map[string]map[string]float64{"scores": {"alice": 1.5}},
	}
	if err := wal.RewriteAll(state, path); err != nil {
		t.Fatal(err)
	}
	set := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "SET"},
		{Type: resp.TypeBulkString, String: "after"},
		{Type: resp.TypeBulkString, String: "rewrite"},
	}}
	if err := wal.Append(walCommand(set)); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	loader, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()
	target := engine.NewConnContext(engine.NewContext(newStore(), nil, nil, engine.SystemConfig{}), nil)
	if err := loader.Load(path, func(command mutation.Command) error {
		engine.ExecuteCommand(target, command)
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

func TestWALRewriteIsDeterministic(t *testing.T) {
	state := store.LogicalSnapshot{
		Keyspace: map[string]store.KeyMetadata{
			"string:b": {Type: store.KeyTypeString},
			"string:a": {Type: store.KeyTypeString},
			"set":      {Type: store.KeyTypeSet},
			"hash":     {Type: store.KeyTypeHash},
			"zset":     {Type: store.KeyTypeSortedSet},
		},
		Strings: map[string]store.StringEntry{
			"string:b": {Value: "two"},
			"string:a": {Value: "one"},
		},
		Sets: map[string]store.SetEntry{"set": {Members: map[string]struct{}{"b": {}, "a": {}}}},
		Hashes: map[string]map[string]string{
			"hash": {"b": "two", "a": "one"},
		},
		SortedSets: map[string]map[string]float64{
			"zset": {"b": 2, "a": 1},
		},
	}
	rewrite := func(name string) []byte {
		t.Helper()
		path := filepath.Join(t.TempDir(), name)
		log, err := wallog.Open(path, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.RewriteAll(state, path); err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	first := rewrite("first.wal")
	second := rewrite("second.wal")
	if !bytes.Equal(first, second) {
		t.Fatal("equivalent logical snapshots produced different WAL bytes")
	}
}

func TestSnapshotRoundTripIncludesTypedSortedSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.vksp")
	snapshotFile := snapshot.New(true)
	defer func() { _ = snapshotFile.Close() }()

	data := snapshot.Data{State: store.LogicalSnapshot{
		Keyspace: map[string]store.KeyMetadata{
			"scores": {Type: store.KeyTypeSortedSet, Version: 3},
		},
		SortedSets: map[string]map[string]float64{"scores": {"alice": 1.5}},
	}}
	if err := snapshotFile.Save(data, path); err != nil {
		t.Fatal(err)
	}
	loaded, err := snapshotFile.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Keyspace["scores"].Type != store.KeyTypeSortedSet {
		t.Fatalf("key type not preserved: %#v", loaded.State.Keyspace["scores"])
	}
	if loaded.State.SortedSets["scores"]["alice"] != 1.5 {
		t.Fatalf("sorted set not preserved: %#v", loaded.State.SortedSets)
	}
}

func TestWALLoadRejectsTruncatedCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.wal")
	if err := os.WriteFile(path, []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nval"), 0o600); err != nil {
		t.Fatal(err)
	}
	wal, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()
	if err := wal.Load(path, func(mutation.Command) error { return nil }); err == nil {
		t.Fatal("truncated WAL should return a recovery error")
	}
}

func TestSnapshotLoadRejectsTruncatedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.vksp")
	if err := os.WriteFile(path, []byte{0x7f, 0x01}, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshotFile := snapshot.New(true)
	defer func() { _ = snapshotFile.Close() }()
	if _, err := snapshotFile.Load(path); err == nil {
		t.Fatal("truncated snapshot should return a recovery error")
	}
}

func TestWALBatchRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	log, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	values := []resp.Value{
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: "a"},
			{Type: resp.TypeBulkString, String: "1"},
		}},
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "DEL"},
			{Type: resp.TypeBulkString, String: "b"},
		}},
	}
	if err := log.AppendBatch(walBatch(values)); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	loader, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()
	var names []string
	if err := loader.Load(path, func(command mutation.Command) error {
		names = append(names, command.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "SET,DEL" {
		t.Fatalf("replayed commands = %v, want [SET DEL]", names)
	}
}

func TestWALLoadRejectsCorruptRecord(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "checksum",
			mutate: func(data []byte) []byte {
				data[len(data)-1] ^= 0xff
				return data
			},
			want: "checksum",
		},
		{
			name: "version",
			mutate: func(data []byte) []byte {
				data[4]++
				return data
			},
			want: "version",
		},
		{
			name: "truncated payload",
			mutate: func(data []byte) []byte {
				return data[:len(data)-1]
			},
			want: "truncated WAL payload",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "valkeydb.wal")
			log, err := wallog.Open(path, true)
			if err != nil {
				t.Fatal(err)
			}
			value := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
				{Type: resp.TypeBulkString, String: "SET"},
				{Type: resp.TypeBulkString, String: "key"},
				{Type: resp.TypeBulkString, String: "value"},
			}}
			if err := log.Append(walCommand(value)); err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, test.mutate(data), 0o600); err != nil {
				t.Fatal(err)
			}

			loader, err := wallog.Open(path, true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = loader.Close() }()
			err = loader.Load(path, func(mutation.Command) error { return nil })
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "offset 0") {
				t.Fatalf("Load() error = %v, want %q at offset 0", err, test.want)
			}
		})
	}
}

func TestWALRepairTailDropsIncompleteFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	log, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	first := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "SET"},
		{Type: resp.TypeBulkString, String: "key"},
		{Type: resp.TypeBulkString, String: "value"},
	}}
	if err := log.Append(walCommand(first)); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("VKWL\x01")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	log, err = wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if err := log.RepairTail(); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := log.Load(path, func(mutation.Command) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed %d commands, want 1", count)
	}
}

func TestBatchReplayFailureDoesNotPartiallyApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	log, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	values := []resp.Value{
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: "key"},
			{Type: resp.TypeBulkString, String: "value"},
		}},
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "ZADD"},
			{Type: resp.TypeBulkString, String: "scores"},
			{Type: resp.TypeBulkString, String: "not-a-score"},
			{Type: resp.TypeBulkString, String: "member"},
		}},
	}
	if err := log.AppendBatch(walBatch(values)); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	loader, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loader.Close() }()
	base := engine.NewContext(newStore(), nil, nil, engine.SystemConfig{})
	err = loader.LoadBatches(path, func(commands []wallog.Command) error {
		batch := make([]engine.QueuedCommand, len(commands))
		copy(batch, commands)
		return base.Replay(batch)
	})
	if err == nil {
		t.Fatal("LoadBatches() accepted invalid batch")
	}
	if _, exists := base.Store.Dictionary.Get("key"); exists {
		t.Fatal("failed batch replay partially applied first command")
	}
}

func TestWALLoadFromCheckpointOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valkeydb.wal")
	log, err := wallog.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	value := func(key string) resp.Value {
		return resp.Value{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: key},
			{Type: resp.TypeBulkString, String: "value"},
		}}
	}
	if err := log.Append(walCommand(value("before"))); err != nil {
		t.Fatal(err)
	}
	offset, err := log.Offset()
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(walCommand(value("after"))); err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := log.LoadBatchesFrom(path, offset, func(commands []wallog.Command) error {
		keys = append(keys, commands[0].Args[0])
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, ",") != "after" {
		t.Fatalf("replayed keys = %v, want [after]", keys)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}
