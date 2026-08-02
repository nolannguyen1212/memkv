package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func TestReceiveRDBFlushesBeforeApplyingSnapshot(t *testing.T) {
	set := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "SET"},
		{Type: resp.TypeBulkString, String: "key"},
		{Type: resp.TypeBulkString, String: "value"},
	}}
	payload := resp.Encode(set)
	stream := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)

	var commands []string
	err := receiveSnapshot(bufio.NewReader(bytes.NewBufferString(stream)), func(name string, _ []resp.Value) error {
		commands = append(commands, name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(commands, []string{"FLUSHALL", "SET"}) {
		t.Fatalf("snapshot dispatch order = %v", commands)
	}
}

func TestReceiveSnapshotValidatesBeforeFlushing(t *testing.T) {
	payload := "not-resp"
	stream := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
	called := false

	err := receiveSnapshot(bufio.NewReader(bytes.NewBufferString(stream)), func(string, []resp.Value) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("receiveSnapshot should reject an invalid payload")
	}
	if called {
		t.Fatal("receiveSnapshot applied commands before validating the payload")
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
	defer manager.Close()
	initial := []byte("initial")
	manager.Propagate(initial)

	serverConnection, replicaConnection := net.Pipe()
	defer func() { _ = replicaConnection.Close() }()
	done := make(chan struct{})
	go func() {
		_ = manager.HandlePSYNC(serverConnection, "replica", manager.primaryStreamID, 0, func() []byte { return nil })
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
