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

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func TestReceiveRDBFlushesBeforeApplyingSnapshot(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
	set := protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, String: "SET"},
		{Type: protocol.TypeBulkString, String: "key"},
		{Type: protocol.TypeBulkString, String: "value"},
	}}
	payload := protocol.Encode(set)
	stream := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)

	var commands []string
	err := manager.receiveRDB(bufio.NewReader(bytes.NewBufferString(stream)), func(name string, _ []protocol.Value) {
		commands = append(commands, name)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(commands, []string{"FLUSHALL", "SET"}) {
		t.Fatalf("snapshot dispatch order = %v", commands)
	}
}

func TestPartialSyncHasNoGapBeforeReplicaRegistration(t *testing.T) {
	manager := NewManager(ManagerConfig{BacklogCapacity: 1024})
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
