package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func TestReceiveSnapshotRestoresLogicalState(t *testing.T) {
	state := store.LogicalSnapshot{
		Keyspace: map[string]store.KeyMetadata{"key": {Type: store.KeyTypeString}},
		Strings:  map[string]store.StringEntry{"key": {Value: "value"}},
	}
	payload, err := snapshot.Encode(snapshot.Data{State: state})
	if err != nil {
		t.Fatal(err)
	}
	stream := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)

	var restored store.LogicalSnapshot
	err = receiveSnapshot(bufio.NewReader(bytes.NewBufferString(stream)), func(state store.LogicalSnapshot) error {
		restored = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Strings["key"].Value != "value" {
		t.Fatalf("restored value = %q, want value", restored.Strings["key"].Value)
	}
}

func TestReceiveSnapshotValidatesBeforeRestore(t *testing.T) {
	payload := "not-snapshot"
	stream := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
	called := false

	err := receiveSnapshot(bufio.NewReader(bytes.NewBufferString(stream)), func(store.LogicalSnapshot) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("receiveSnapshot should reject an invalid payload")
	}
	if called {
		t.Fatal("receiveSnapshot restored state before validating the payload")
	}
}

func TestParseFullResync(t *testing.T) {
	replicationID, offset, err := parseFullResync("+FULLRESYNC stream-id 42")
	if err != nil {
		t.Fatal(err)
	}
	if replicationID != "stream-id" || offset != 42 {
		t.Fatalf("parseFullResync() = %q, %d", replicationID, offset)
	}
}

func TestPartialSyncHasNoGapBeforeReplicaRegistration(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	defer func() { _ = manager.Close() }()
	initial := []byte("initial")
	manager.Propagate(initial)

	serverConnection, replicaConnection := net.Pipe()
	defer func() { _ = replicaConnection.Close() }()
	done := make(chan struct{})
	go func() {
		_ = manager.HandlePSYNC(serverConnection, "replica", manager.primaryStreamID, 0, func() store.LogicalSnapshot { return store.LogicalSnapshot{} })
		close(done)
	}()

	prefix := []byte("+CONTINUE " + manager.primaryStreamID + "\r\n")
	received := make([]byte, len(prefix)+len(initial))
	if _, err := io.ReadFull(replicaConnection, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, append(prefix, initial...)) {
		t.Fatalf("partial sync payload = %q", received)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("partial sync did not register replica")
	}

	next := []byte("next")
	propagated := make(chan struct{})
	go func() {
		manager.Propagate(next)
		close(propagated)
	}()
	received = make([]byte, len(next))
	if _, err := io.ReadFull(replicaConnection, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, next) {
		t.Fatalf("post-sync propagation = %q", received)
	}
	select {
	case <-propagated:
	case <-time.After(time.Second):
		t.Fatal("propagation did not finish")
	}
}

func TestStreamAppliesCommittedBatchAsOneUnit(t *testing.T) {
	commands := []resp.Value{
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: "first"},
			{Type: resp.TypeBulkString, String: "1"},
		}},
		{Type: resp.TypeArray, Array: []resp.Value{
			{Type: resp.TypeBulkString, String: "SET"},
			{Type: resp.TypeBulkString, String: "second"},
			{Type: resp.TypeBulkString, String: "2"},
		}},
	}
	payload := resp.Encode(resp.Value{Type: resp.TypeArray, Array: commands})
	session := &replicaSession{reader: bufio.NewReader(bytes.NewBufferString(payload))}
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	defer func() { _ = manager.Close() }()
	standaloneCalled := false
	var batch []Command
	err := manager.streamCommands(session, func(string, []resp.Value) error {
		standaloneCalled = true
		return nil
	}, func(commands []Command) error {
		batch = commands
		return nil
	})
	if err == nil {
		t.Fatal("streamCommands() accepted EOF")
	}
	if standaloneCalled {
		t.Fatal("batch was applied as standalone commands")
	}
	if len(batch) != 2 || batch[0].Name != "SET" || batch[1].Name != "SET" {
		t.Fatalf("applied batch = %#v", batch)
	}
	if manager.replicationOffset.Load() != int64(len(payload)) {
		t.Fatalf("replication offset = %d, want %d", manager.replicationOffset.Load(), len(payload))
	}
}

func TestSlowReplicaQueueOverflowDisconnectsReplica(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	defer func() { _ = manager.Close() }()
	primaryConnection, replicaConnection := net.Pipe()
	defer func() { _ = replicaConnection.Close() }()
	replica := newReplicaConnection("slow", primaryConnection)
	manager.addReplica(replica)
	for range defaultReplicaQueueCapacity {
		if !replica.enqueue([]byte("queued")) {
			t.Fatal("replica queue filled before capacity")
		}
	}
	manager.Propagate([]byte("overflow"))
	if len(manager.ActiveReplicas()) != 0 {
		t.Fatal("slow replica remained registered after queue overflow")
	}
}

func TestReplicationIDChangesForNewPrimaryHistory(t *testing.T) {
	first := NewManager(ManagerConfig{BacklogCapacity: 1, ListeningAddress: "127.0.0.1:6379"})
	second := NewManager(ManagerConfig{BacklogCapacity: 1, ListeningAddress: "127.0.0.1:6379"})
	defer func() { _ = first.Close() }()
	defer func() { _ = second.Close() }()
	if first.primaryStreamID == second.primaryStreamID {
		t.Fatalf("replication IDs match: %q", first.primaryStreamID)
	}
}

func TestFullSyncAndLiveStreamConverge(t *testing.T) {
	primary := NewManager(ManagerConfig{BacklogCapacity: 1024, ListeningAddress: ":6379"})
	defer func() { _ = primary.Close() }()
	source := store.New(store.Config{})
	source.SetString("initial", "one", time.Time{})
	target := store.New(store.Config{})

	serverConnection, replicaConnection := net.Pipe()
	defer func() { _ = replicaConnection.Close() }()
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.fullSync(newReplicaConnection("replica", serverConnection), source.Snapshot)
	}()
	reader := bufio.NewReader(replicaConnection)
	header, err := readLine(reader)
	if err != nil || !strings.HasPrefix(header, "+FULLRESYNC ") {
		t.Fatalf("full sync header = %q, error = %v", header, err)
	}
	if err := receiveSnapshot(reader, func(state store.LogicalSnapshot) error {
		return target.Restore(state, target.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}

	live := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "SET"},
		{Type: resp.TypeBulkString, String: "live"},
		{Type: resp.TypeBulkString, String: "two"},
	}}
	applied := make(chan error, 1)
	mutationApplied := make(chan struct{})
	replica := NewManager(ManagerConfig{BacklogCapacity: 1})
	defer func() { _ = replica.Close() }()
	go func() {
		session := &replicaSession{reader: reader}
		applied <- replica.streamCommands(session, func(name string, args []resp.Value) error {
			if name != "SET" {
				return fmt.Errorf("unexpected command %s", name)
			}
			target.SetString(args[0].String, args[1].String, time.Time{})
			close(mutationApplied)
			return nil
		}, func([]Command) error { return nil })
	}()
	primary.Propagate([]byte(resp.Encode(live)))
	select {
	case <-mutationApplied:
	case err := <-applied:
		t.Fatalf("stream ended before apply: %v", err)
	case <-time.After(time.Second):
		t.Fatal("live mutation was not applied")
	}
	if value, exists := target.Dictionary.Get("live"); !exists || value != "two" {
		t.Fatalf("live value = %q, exists = %v", value, exists)
	}
	if value, exists := target.Dictionary.Get("initial"); !exists || value != "one" {
		t.Fatalf("initial value = %q, exists = %v", value, exists)
	}
}
