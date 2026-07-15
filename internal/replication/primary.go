package replication

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func (manager *Manager) HandlePSYNC(connection net.Conn, replicaAddress, replicaID string, offset int64, getRDB func() []byte) error {
	// Keep backlog inspection, snapshot transfer, and replica registration in
	// the same ordering boundary as propagation. Otherwise writes that arrive
	// between sending the backlog/snapshot and AddReplica are lost by the new
	// replica.
	manager.propagateMutex.Lock()
	defer manager.propagateMutex.Unlock()

	replicaConnection := &ReplicaConnection{
		ID:         generateHex(8),
		Address:    replicaAddress,
		Connection: connection,
	}

	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicaID != "?" && replicaID == streamID && manager.backlog.CanServe(offset) {
		return manager.partialSync(connection, replicaConnection, offset)
	}
	return manager.fullSync(connection, replicaConnection, getRDB)
}

func (manager *Manager) fullSync(connection net.Conn, replicaConnection *ReplicaConnection, getRDB func() []byte) error {
	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	offset := manager.backlog.CurrentOffset()
	rdbData := getRDB()

	log.Printf("replication: full sync to replica %s (rdb=%d bytes, offset=%d)", replicaConnection.Address, len(rdbData), offset)

	if _, err := fmt.Fprintf(connection, "+FULLRESYNC %s %d\r\n", streamID, offset); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(connection, "$%d\r\n", len(rdbData)); err != nil {
		return err
	}
	if err := writeAll(connection, rdbData); err != nil {
		return err
	}
	if err := writeAll(connection, []byte("\r\n")); err != nil {
		return err
	}

	manager.AddReplica(replicaConnection)
	go manager.readReplicaCommands(connection, replicaConnection)
	return nil
}

func (manager *Manager) partialSync(connection net.Conn, replicaConnection *ReplicaConnection, fromOffset int64) error {
	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	data := manager.backlog.ReadFrom(fromOffset)
	log.Printf("replication: partial sync to replica %s (from offset=%d, delta=%d bytes)", replicaConnection.Address, fromOffset, len(data))

	if _, err := fmt.Fprintf(connection, "+CONTINUE %s\r\n", streamID); err != nil {
		return err
	}
	if len(data) > 0 {
		if err := writeAll(connection, data); err != nil {
			return err
		}
	}

	manager.AddReplica(replicaConnection)
	go manager.readReplicaCommands(connection, replicaConnection)
	return nil
}

func writeAll(connection net.Conn, data []byte) error {
	for len(data) > 0 {
		written, err := connection.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func (manager *Manager) readReplicaCommands(connection net.Conn, replicaConnection *ReplicaConnection) {
	defer func() { _ = connection.Close() }()
	defer func() {
		log.Printf("replication: replica %s disconnected", replicaConnection.Address)
		manager.RemoveReplica(replicaConnection.ID)
	}()

	reader := bufio.NewReader(connection)
	for {
		value, err := protocol.Decode(reader)
		if err != nil {
			log.Printf("replication: read error from replica %s: %v", replicaConnection.Address, err)
			return
		}
		if value.Type != protocol.TypeArray || len(value.Array) < 2 {
			continue
		}
		if strings.ToUpper(value.Array[0].String) != "REPLCONF" {
			continue
		}

		switch strings.ToUpper(value.Array[1].String) {
		case "HEARTBEAT":
			manager.UpdateReplicaHeartbeat(replicaConnection.ID)
		case "ACK":
			if len(value.Array) >= 3 {
				offset, err := strconv.ParseInt(value.Array[2].String, 10, 64)
				if err == nil {
					replicaConnection.AcknowledgedOffset.Store(offset)
				}
			}
		}
	}
}

func (manager *Manager) StartLivenessMonitor(stopChan <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			manager.evictDeadReplicas()
		}
	}
}

func (manager *Manager) evictDeadReplicas() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	deadline := time.Now().Add(-manager.config.HeartbeatTimeout)
	for replicaID, lastHeartbeat := range manager.replicaLastHeartbeat {
		if lastHeartbeat.Before(deadline) {
			for i, replica := range manager.replicas {
				if replica.ID == replicaID {
					manager.replicas = append(manager.replicas[:i], manager.replicas[i+1:]...)
					break
				}
			}
			delete(manager.replicaLastHeartbeat, replicaID)
		}
	}
}
