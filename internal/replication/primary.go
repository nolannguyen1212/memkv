package replication

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func (manager *Manager) HandlePSYNC(connection net.Conn, replicaAddress, replicaID string, offset int64, getRDB func() []byte) error {
	replicaConnection := &ReplicaConnection{
		ID:         generateHex(8),
		Address:    replicaAddress,
		Connection: connection,
	}

	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicaID != "?" && replicaID == streamID && manager.backlog.CanServe(offset) {
		manager.partialSync(connection, replicaConnection, offset)
	} else {
		manager.fullSync(connection, replicaConnection, getRDB)
	}
	return nil
}

func (manager *Manager) fullSync(connection net.Conn, replicaConnection *ReplicaConnection, getRDB func() []byte) {
	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	offset := manager.backlog.CurrentOffset()
	rdbData := getRDB()

	log.Printf("replication: full sync to replica %s (rdb=%d bytes, offset=%d)", replicaConnection.Address, len(rdbData), offset)

	fmt.Fprintf(connection, "+FULLRESYNC %s %d\r\n", streamID, offset)
	fmt.Fprintf(connection, "$%d\r\n", len(rdbData))
	connection.Write(rdbData)
	connection.Write([]byte("\r\n"))

	manager.AddReplica(replicaConnection)
	go manager.readReplicaCommands(connection, replicaConnection)
}

func (manager *Manager) partialSync(connection net.Conn, replicaConnection *ReplicaConnection, fromOffset int64) {
	manager.mutex.RLock()
	streamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	data := manager.backlog.ReadFrom(fromOffset)
	log.Printf("replication: partial sync to replica %s (from offset=%d, delta=%d bytes)", replicaConnection.Address, fromOffset, len(data))

	fmt.Fprintf(connection, "+CONTINUE %s\r\n", streamID)
	if len(data) > 0 {
		connection.Write(data)
	}

	manager.AddReplica(replicaConnection)
	go manager.readReplicaCommands(connection, replicaConnection)
}

func (manager *Manager) readReplicaCommands(connection net.Conn, replicaConnection *ReplicaConnection) {
	defer connection.Close()
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
