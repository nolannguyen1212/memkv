package replication

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func (manager *Manager) HandlePSYNC(connection net.Conn, replicaAddress, replicationID string, offset int64, getSnapshot func() store.LogicalSnapshot) error {
	manager.propagateMutex.Lock()
	defer manager.propagateMutex.Unlock()

	replica := newReplicaConnection(replicaAddress, connection)

	manager.mutex.RLock()
	primaryStreamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicationID == primaryStreamID && manager.backlog.CanServe(offset) {
		return manager.partialSync(replica, offset)
	}
	return manager.fullSync(replica, getSnapshot)
}

func (manager *Manager) fullSync(replica *replicaConnection, getSnapshot func() store.LogicalSnapshot) error {
	manager.mutex.RLock()
	primaryStreamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	offset := manager.backlog.CurrentOffset()
	payload, err := snapshot.Encode(snapshot.Data{State: getSnapshot(), IncludedOffset: offset})
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	log.Printf("replication: full sync to %s, snapshot=%d bytes, offset=%d", replica.Address, len(payload), offset)

	header := fmt.Sprintf("+FULLRESYNC %s %d\r\n$%d\r\n", primaryStreamID, offset, len(payload))
	if err := writeAll(replica.Connection, []byte(header)); err != nil {
		return err
	}
	if err := writeAll(replica.Connection, payload); err != nil {
		return err
	}
	if err := writeAll(replica.Connection, []byte("\r\n")); err != nil {
		return err
	}

	manager.registerReplica(replica)
	return nil
}

func (manager *Manager) partialSync(replica *replicaConnection, offset int64) error {
	manager.mutex.RLock()
	primaryStreamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	delta := manager.backlog.ReadFrom(offset)
	log.Printf("replication: partial sync to %s, offset=%d, delta=%d bytes", replica.Address, offset, len(delta))

	header := fmt.Sprintf("+CONTINUE %s\r\n", primaryStreamID)
	if err := writeAll(replica.Connection, []byte(header)); err != nil {
		return err
	}
	if err := writeAll(replica.Connection, delta); err != nil {
		return err
	}

	manager.registerReplica(replica)
	return nil
}

func (manager *Manager) registerReplica(replica *replicaConnection) {
	manager.addReplica(replica)
	manager.waitGroup.Go(func() {
		manager.watchReplica(replica)
	})
	manager.waitGroup.Go(func() {
		manager.writeReplica(replica)
	})
}

func (manager *Manager) watchReplica(replica *replicaConnection) {
	_, _ = io.Copy(io.Discard, replica.Connection)
	manager.removeReplica(replica)
	log.Printf("replication: replica %s disconnected", replica.Address)
}

func (manager *Manager) writeReplica(replica *replicaConnection) {
	for {
		select {
		case data := <-replica.queue:
			if err := writeAll(replica.Connection, data); err != nil {
				manager.removeReplica(replica)
				return
			}
		case <-replica.done:
			return
		}
	}
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
