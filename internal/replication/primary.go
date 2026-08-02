package replication

import (
	"fmt"
	"io"
	"net"

	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func (manager *Manager) HandlePSYNC(
	connection net.Conn,
	replicaAddress string,
	replicationID string,
	offset int64,
	captureSnapshot SnapshotCapture,
) error {
	replica := newReplicaConnection(replicaAddress, connection)
	manager.mutex.RLock()
	primaryStreamID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicationID == primaryStreamID {
		delta, registered := manager.registerReplica(replica, offset)

		if registered {
			if err := manager.sendPartialSync(replica, primaryStreamID, offset, delta); err != nil {
				manager.removeReplica(replica)
				return err
			}

			return manager.activateReplica(replica)
		}
	}

	return manager.fullSync(replica, primaryStreamID, captureSnapshot)
}

func (manager *Manager) registerReplica(
	replica *replicaConnection,
	offset int64,
) ([]byte, bool) {
	manager.propagateMutex.Lock()
	defer manager.propagateMutex.Unlock()

	if !manager.backlog.CanServe(offset) {
		return nil, false
	}

	delta := manager.backlog.ReadFrom(offset)
	manager.addReplica(replica)
	return delta, true
}

func (manager *Manager) fullSync(
	replica *replicaConnection,
	primaryStreamID string,
	captureSnapshot SnapshotCapture,
) error {
	state, syncOffset, err := captureSnapshot()

	if err != nil {
		return fmt.Errorf("capture snapshot: %w", err)
	}

	delta, registered := manager.registerReplica(replica, syncOffset)

	if !registered {
		return fmt.Errorf("snapshot offset %d is no longer available in replication backlog", syncOffset)
	}

	if err := manager.sendFullSync(replica, primaryStreamID, syncOffset, state); err != nil {
		manager.removeReplica(replica)
		return err
	}

	if err := writeAll(replica.Connection, delta); err != nil {
		manager.removeReplica(replica)
		return err
	}

	return manager.activateReplica(replica)
}

func (manager *Manager) sendFullSync(
	replica *replicaConnection,
	primaryStreamID string,
	offset int64,
	state store.LogicalSnapshot,
) error {
	payload, err := snapshot.Encode(snapshot.Data{State: state, IncludedOffset: offset})

	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	manager.logger.Info(
		"replication full sync",
		"replica", replica.Address,
		"snapshot_bytes", len(payload),
		"offset", offset,
	)
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

func (manager *Manager) sendPartialSync(
	replica *replicaConnection,
	primaryStreamID string,
	offset int64,
	delta []byte,
) error {
	manager.logger.Info(
		"replication partial sync",
		"replica", replica.Address,
		"offset", offset,
		"delta_bytes", len(delta),
	)

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
