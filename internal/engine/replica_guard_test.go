package engine

import (
	"net"
	"strings"
	"testing"

	"github.com/william1nguyen/memkv/internal/mutation"
	"github.com/william1nguyen/memkv/internal/store"
)

type replicaReplicator struct {
	psyncCalls int
}

func (*replicaReplicator) ActiveReplicas() []string {
	return nil
}

func (replicator *replicaReplicator) HandlePSYNC(
	net.Conn,
	string,
	string,
	int64,
	SnapshotCapture,
) error {
	replicator.psyncCalls++
	return nil
}

func (*replicaReplicator) Info() string {
	return ""
}

func (*replicaReplicator) IsReplica() bool {
	return true
}

func (*replicaReplicator) Offset() int64 {
	return 0
}

func (*replicaReplicator) Propagate(mutation.Batch) {}

func TestReplicaRejectsQueuedWrites(t *testing.T) {
	replicator := &replicaReplicator{}
	base := NewContext(store.New(testStoreConfig), nil, replicator, SystemConfig{})
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	connection := NewConnContext(base, client)

	if result := exec(connection, "MULTI"); result.Type != ResultSimpleString {
		t.Fatalf("MULTI result = %#v", result)
	}

	result := exec(connection, "SET", "key", "value")

	if result.Type != ResultError || !strings.HasPrefix(result.String, "READONLY") {
		t.Fatalf("SET result = %#v, want READONLY", result)
	}

	if len(connection.Transaction.Queue) != 0 {
		t.Fatalf("queued commands = %v, want none", connection.Transaction.Queue)
	}

	result = exec(connection, "EXEC")

	if result.Type != ResultArray || len(result.Array) != 0 {
		t.Fatalf("EXEC result = %#v, want empty array", result)
	}

	if _, exists := connection.Store.Dictionary.Get("key"); exists {
		t.Fatal("replica applied a transaction write")
	}
}

func TestReplicaRejectsPSYNC(t *testing.T) {
	replicator := &replicaReplicator{}
	base := NewContext(store.New(testStoreConfig), nil, replicator, SystemConfig{})
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	connection := NewConnContext(base, client)

	result := connection.SynchronizeReplica([]string{"?", "-1"})

	if result.Type != ResultError || !strings.Contains(result.String, "only available on a primary") {
		t.Fatalf("PSYNC result = %#v", result)
	}

	if replicator.psyncCalls != 0 {
		t.Fatalf("HandlePSYNC calls = %d, want 0", replicator.psyncCalls)
	}
}
