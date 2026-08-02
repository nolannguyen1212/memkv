package replication

import (
	"fmt"
	"io"
	"net"

	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func (manager *Manager) HandlePSYNC(connection net.Conn, replicaAddress, replicationID string, offset int64, getSnapshot func() (store.LogicalSnapshot, error)) error {
	manager.propagateMutex.Lock()
	replica := newReplicaConnection(replicaAddress, connection)
	manager.mutex.RLock()
	primaryStreamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicationID == primaryStreamID && manager.backlog.CanServe(offset) {
		delta := manager.backlog.ReadFrom(offset)
		manager.addReplica(replica)
		manager.propagateMutex.Unlock()
		if err := manager.sendPartialSync(replica, primaryStreamID, offset, delta); err != nil {
			manager.removeReplica(replica)
			return err
		}
		return manager.activateReplica(replica)
	}
	syncOffset := manager.backlog.CurrentOffset()
	state, err := getSnapshot()
	if err != nil {
		manager.propagateMutex.Unlock()
		return fmt.Errorf("capture snapshot: %w", err)
	}
	manager.addReplica(replica)
	manager.propagateMutex.Unlock()
	if err := manager.sendFullSync(replica, primaryStreamID, syncOffset, state); err != nil {
		manager.removeReplica(replica)
		return err
	}
	return manager.activateReplica(replica)
}

func (manager *Manager) sendFullSync(replica *replicaConnection, primaryStreamID string, offset int64, state store.LogicalSnapshot) error {
	payload, err := snapshot.Encode(snapshot.Data{State: state, IncludedOffset: offset})
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	manager.logger.Info("replication full sync", "replica", replica.Address, "snapshot_bytes", len(payload), "offset", offset)
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

	return nil
}

func (manager *Manager) sendPartialSync(replica *replicaConnection, primaryStreamID string, offset int64, delta []byte) error {
	manager.logger.Info("replication partial sync", "replica", replica.Address, "offset", offset, "delta_bytes", len(delta))

	header := fmt.Sprintf("+CONTINUE %s\r\n", primaryStreamID)
	if err := writeAll(replica.Connection, []byte(header)); err != nil {
		return err
	}
	if err := writeAll(replica.Connection, delta); err != nil {
		return err
	}

	return nil
}

func (manager *Manager) activateReplica(replica *replicaConnection) error {
	select {
	case <-replica.done:
		return net.ErrClosed
	default:
	}
	manager.waitGroup.Go(func() {
		manager.watchReplica(replica)
	})
	manager.waitGroup.Go(func() {
		manager.writeReplica(replica)
	})
	return nil
}

func (manager *Manager) watchReplica(replica *replicaConnection) {
	_, _ = io.Copy(io.Discard, replica.Connection)
	manager.removeReplica(replica)
	manager.logger.Info("replica disconnected", "replica", replica.Address)
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
