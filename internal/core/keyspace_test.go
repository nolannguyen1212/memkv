package core

import (
	"bufio"
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

type recordingAppender struct {
	mutex  sync.Mutex
	values []protocol.Value
}

type failingAppender struct{}

func (failingAppender) Append(protocol.Value) error { return errors.New("disk full") }

func (appender *recordingAppender) Append(value protocol.Value) error {
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
		if len(value.Array) > 0 {
			result = append(result, value.Array[0].String)
		}
	}
	return result
}

func TestTypedKeyspaceRejectsWrongTypeAndSetReplacesType(t *testing.T) {
	conn := newTestContext()

	if result := exec(conn, "SADD", "shared", "member"); result.Type != protocol.TypeInteger || result.Number != 1 {
		t.Fatalf("SADD failed: %#v", result)
	}
	if result := exec(conn, "GET", "shared"); result.Type != protocol.TypeError {
		t.Fatalf("GET against set should return WRONGTYPE, got %#v", result)
	}
	if result := exec(conn, "LPUSH", "shared", "item"); result.Type != protocol.TypeError {
		t.Fatalf("LPUSH against set should return WRONGTYPE, got %#v", result)
	}

	if result := exec(conn, "SET", "shared", "value"); result.Type != protocol.TypeSimpleString {
		t.Fatalf("SET should replace the old type, got %#v", result)
	}
	if result := exec(conn, "SCARD", "shared"); result.Type != protocol.TypeError {
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
	conn := NewConnContext(NewContext(NewStore(testStoreConfig), appender, nil), nil)

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
	reader := bufio.NewReader(bytes.NewReader(source.GenerateRDB()))
	for {
		value, err := protocol.Decode(reader)
		if err != nil {
			break
		}
		Execute(target, value.Array[0].String, value.Array[1:])
	}

	if score, exists := target.Store.SortedSet.Score("scores", "alice"); !exists || score != 1.5 {
		t.Fatalf("sorted set missing after snapshot replay: score=%v exists=%v", score, exists)
	}
	if score, exists := target.Store.SortedSet.Score("scores", "bob"); !exists || score != 2 {
		t.Fatalf("sorted set missing after snapshot replay: score=%v exists=%v", score, exists)
	}
}

func TestPersistenceFailureStopsFurtherWrites(t *testing.T) {
	base := NewContext(NewStore(testStoreConfig), failingAppender{}, nil)
	conn := NewConnContext(base, nil)
	if result := exec(conn, "SET", "first", "value"); result.Type != protocol.TypeError {
		t.Fatalf("failed AOF append should fail the command, got %#v", result)
	}
	if result := exec(conn, "SET", "second", "value"); result.Type != protocol.TypeError || result.String == "" {
		t.Fatalf("writes should remain disabled after persistence failure, got %#v", result)
	}
	if _, exists := conn.Store.Dictionary.Get("second"); exists {
		t.Fatal("write executed after persistence entered fail-stop mode")
	}
}
