package replication

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/william1nguyen/memkv/internal/mutation"
	"github.com/william1nguyen/memkv/internal/resp"
	"github.com/william1nguyen/memkv/internal/snapshot"
	"github.com/william1nguyen/memkv/internal/store"
)

func closeManagerAfterTest(t *testing.T, manager *Manager) {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close replication manager: %v", err)
		}
	})
}

func closeConnectionAfterTest(t *testing.T, connection net.Conn) {
	t.Helper()
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
}

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

func TestBacklogWrapsWithAbsoluteOffsets(t *testing.T) {
	backlog := NewBacklog(4)
	backlog.Write([]byte("abcdef"))

	if backlog.CurrentOffset() != 6 {
		t.Fatalf("current offset = %d, want 6", backlog.CurrentOffset())
	}

	for _, offset := range []int64{0, 1, 7} {
		if backlog.CanServe(offset) {
			t.Fatalf("CanServe(%d) = true", offset)
		}
	}

	for _, offset := range []int64{2, 4, 6} {
		if !backlog.CanServe(offset) {
			t.Fatalf("CanServe(%d) = false", offset)
		}
	}

	if actual := string(backlog.ReadFrom(2)); actual != "cdef" {
		t.Fatalf("ReadFrom(2) = %q, want cdef", actual)
	}

	if actual := string(backlog.ReadFrom(4)); actual != "ef" {
		t.Fatalf("ReadFrom(4) = %q, want ef", actual)
	}
}

func TestStaleOffsetFallsBackToFullSync(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 4})
	closeManagerAfterTest(t, manager)
	manager.propagate([]byte("abcdef"))
	serverConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, replicaConnection)
	done := make(chan error, 1)
	capture := func() (store.LogicalSnapshot, int64, error) {
		return store.LogicalSnapshot{}, manager.Offset(), nil
	}

	go func() {
		done <- manager.HandlePSYNC(
			serverConnection,
			"replica",
			manager.primaryStreamID,
			1,
			capture,
		)
	}()
	reader := bufio.NewReader(replicaConnection)
	header, err := readLine(reader)

	if err != nil || !strings.HasPrefix(header, "+FULLRESYNC ") {
		t.Fatalf("header = %q, error = %v", header, err)
	}

	if err := receiveSnapshot(reader, func(store.LogicalSnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPartialSyncHasNoGapBeforeReplicaRegistration(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	closeManagerAfterTest(t, manager)
	initial := []byte("initial")
	manager.propagate(initial)

	serverConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, replicaConnection)
	done := make(chan struct{})
	capture := func() (store.LogicalSnapshot, int64, error) {
		return store.LogicalSnapshot{}, manager.Offset(), nil
	}

	go func() {
		_ = manager.HandlePSYNC(
			serverConnection,
			"replica",
			manager.primaryStreamID,
			0,
			capture,
		)
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
		manager.propagate(next)
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
	closeManagerAfterTest(t, manager)
	var batch []Command
	err := manager.streamCommands(session, func(commands mutation.Batch) error {
		batch = commands
		return nil
	})

	if err == nil {
		t.Fatal("streamCommands() accepted EOF")
	}

	if len(batch) != 2 {
		t.Fatalf("batch length = %d, want 2", len(batch))
	}

	if batch[0].Name != "SET" {
		t.Fatalf("first command = %#v, want SET", batch[0])
	}

	if batch[1].Name != "SET" {
		t.Fatalf("second command = %#v, want SET", batch[1])
	}

	if manager.replicationOffset.Load() != int64(len(payload)) {
		t.Fatalf("replication offset = %d, want %d", manager.replicationOffset.Load(), len(payload))
	}
}

func TestSlowReplicaQueueOverflowDisconnectsReplica(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	closeManagerAfterTest(t, manager)
	primaryConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, replicaConnection)
	replica := newReplicaConnection("slow", primaryConnection)
	manager.addReplica(replica)

	for range defaultReplicaQueueCapacity {
		if !replica.enqueue([]byte("queued")) {
			t.Fatal("replica queue filled before capacity")
		}
	}

	manager.propagate([]byte("overflow"))

	if len(manager.ActiveReplicas()) != 0 {
		t.Fatal("slow replica remained registered after queue overflow")
	}
}

func TestReplicationIDChangesForNewPrimaryHistory(t *testing.T) {
	first := NewManager(ManagerConfig{BacklogCapacity: 1, ListeningAddress: "127.0.0.1:6379"})
	second := NewManager(ManagerConfig{BacklogCapacity: 1, ListeningAddress: "127.0.0.1:6379"})
	closeManagerAfterTest(t, first)
	closeManagerAfterTest(t, second)

	if first.primaryStreamID == second.primaryStreamID {
		t.Fatalf("replication IDs match: %q", first.primaryStreamID)
	}
}

func TestFullSyncClearsDeadlineAndLiveStreamConverges(t *testing.T) {
	primary := NewManager(ManagerConfig{BacklogCapacity: 1024, ListeningAddress: ":6379"})
	closeManagerAfterTest(t, primary)
	source := store.New(store.Config{})
	source.SetString("initial", "one", time.Time{})
	target := store.New(store.Config{})

	serverConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, replicaConnection)

	if err := serverConnection.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.HandlePSYNC(serverConnection, "replica", "?", -1, func() (store.LogicalSnapshot, int64, error) {
			return source.Snapshot(), primary.Offset(), nil
		})
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

	applied := make(chan error, 1)
	mutationApplied := make(chan struct{})
	replica := NewManager(ManagerConfig{BacklogCapacity: 1})
	closeManagerAfterTest(t, replica)
	go func() {
		session := &replicaSession{reader: reader}
		applied <- replica.streamCommands(session, func(commands mutation.Batch) error {
			command := commands[0]

			if command.Name != "SET" {
				return fmt.Errorf("unexpected command %s", command.Name)
			}

			target.SetString(command.Args[0], command.Args[1], time.Time{})
			close(mutationApplied)
			return nil
		})
	}()
	primary.Propagate(mutation.Batch{mutation.New("SET", "live", "two")})

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

func TestFullSyncSocketWriteDoesNotBlockPropagation(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	closeManagerAfterTest(t, manager)
	serverConnection, replicaConnection := net.Pipe()
	snapshotCaptured := make(chan struct{})
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- manager.HandlePSYNC(serverConnection, "slow", "?", -1, func() (store.LogicalSnapshot, int64, error) {
			close(snapshotCaptured)
			return store.LogicalSnapshot{}, manager.Offset(), nil
		})
	}()

	select {
	case <-snapshotCaptured:
	case <-time.After(time.Second):
		t.Fatal("snapshot was not captured")
	}

	propagated := make(chan struct{})
	go func() {
		manager.Propagate(mutation.Batch{mutation.New("SET", "key", "value")})
		close(propagated)
	}()

	select {
	case <-propagated:
	case <-time.After(time.Second):
		t.Fatal("propagation blocked on full-sync socket write")
	}

	if err := replicaConnection.Close(); err != nil {
		t.Fatal(err)
	}

	if err := <-syncDone; err == nil {
		t.Fatal("full sync succeeded after peer closed without reading")
	}
}

func TestFullSyncSnapshotCaptureDoesNotBlockPropagation(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	closeManagerAfterTest(t, manager)
	serverConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, replicaConnection)

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- manager.HandlePSYNC(serverConnection, "replica", "?", -1, func() (store.LogicalSnapshot, int64, error) {
			offset := manager.Offset()
			propagated := make(chan struct{})
			go func() {
				manager.Propagate(mutation.Batch{mutation.New("SET", "during-sync", "value")})
				close(propagated)
			}()

			select {
			case <-propagated:
				return store.LogicalSnapshot{}, offset, nil
			case <-time.After(time.Second):
				return store.LogicalSnapshot{}, 0, errors.New("snapshot capture blocked propagation")
			}
		})
	}()

	reader := bufio.NewReader(replicaConnection)
	header, err := readLine(reader)

	if err != nil || !strings.HasPrefix(header, "+FULLRESYNC ") {
		t.Fatalf("full sync header = %q, error = %v", header, err)
	}

	if err := receiveSnapshot(reader, func(store.LogicalSnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}

	value, err := resp.Decode(reader)

	if err != nil {
		t.Fatal(err)
	}

	command, err := decodeCommand(value)

	if err != nil {
		t.Fatal(err)
	}

	if command.Name != "SET" || command.Args[0] != "during-sync" {
		t.Fatalf("catch-up command = %#v", command)
	}

	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCaptureFailureDoesNotRegisterReplica(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	closeManagerAfterTest(t, manager)
	serverConnection, replicaConnection := net.Pipe()
	closeConnectionAfterTest(t, serverConnection)
	closeConnectionAfterTest(t, replicaConnection)
	expected := errors.New("snapshot unavailable")
	err := manager.HandlePSYNC(serverConnection, "replica", "?", -1, func() (store.LogicalSnapshot, int64, error) {
		return store.LogicalSnapshot{}, 0, expected
	})

	if !errors.Is(err, expected) {
		t.Fatalf("HandlePSYNC() error = %v, want %v", err, expected)
	}

	if len(manager.ActiveReplicas()) != 0 {
		t.Fatal("replica registered after snapshot capture failure")
	}
}
