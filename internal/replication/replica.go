package replication

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

const reconnectDelay = 2 * time.Second

func (manager *Manager) connectToPrimary(ctx context.Context, address, username, password string, apply ApplyCommand, applyBatch ApplyBatch, restore ApplySnapshot) error {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()

	if !manager.setPrimaryConnection(ctx, connection) {
		return ctx.Err()
	}
	defer manager.clearPrimaryConnection(connection)

	session := newReplicaSession(connection)
	if err := manager.handshake(session, username, password); err != nil {
		return err
	}
	if err := manager.synchronize(session, restore); err != nil {
		return err
	}
	return manager.streamCommands(session, apply, applyBatch)
}

func (manager *Manager) setPrimaryConnection(ctx context.Context, connection net.Conn) bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if ctx.Err() != nil {
		return false
	}
	manager.primaryConnection = connection
	return true
}

func (manager *Manager) clearPrimaryConnection(connection net.Conn) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.primaryConnection == connection {
		manager.primaryConnection = nil
	}
}

func (manager *Manager) streamCommands(session *replicaSession, apply ApplyCommand, applyBatch ApplyBatch) error {
	for {
		value, err := resp.Decode(session.reader)
		if err != nil {
			return err
		}
		if value.Type != resp.TypeArray || len(value.Array) == 0 {
			return fmt.Errorf("replication stream contains a non-command value")
		}

		if value.Array[0].Type == resp.TypeArray {
			commands, err := decodeBatch(value)
			if err != nil {
				return err
			}
			if err := applyBatch(commands); err != nil {
				return fmt.Errorf("apply replication batch: %w", err)
			}
		} else {
			name := strings.ToUpper(value.Array[0].String)
			if err := apply(name, value.Array[1:]); err != nil {
				return fmt.Errorf("apply %s: %w", name, err)
			}
		}
		manager.replicationOffset.Add(int64(len(resp.Encode(value))))
	}
}

func (manager *Manager) reconnectLoop(ctx context.Context, address, username, password string, apply ApplyCommand, applyBatch ApplyBatch, restore ApplySnapshot) {
	for {
		if err := manager.connectToPrimary(ctx, address, username, password, apply, applyBatch, restore); err != nil && ctx.Err() == nil {
			log.Printf("replication: primary %s disconnected: %v", address, err)
		}
		if !waitForReconnect(ctx) {
			return
		}
	}
}

func decodeBatch(value resp.Value) ([]Command, error) {
	commands := make([]Command, len(value.Array))
	for index, item := range value.Array {
		if item.Type != resp.TypeArray || len(item.Array) == 0 {
			return nil, fmt.Errorf("replication batch command %d is invalid", index)
		}
		name := item.Array[0]
		if name.Type != resp.TypeBulkString && name.Type != resp.TypeSimpleString {
			return nil, fmt.Errorf("replication batch command %d has invalid name", index)
		}
		commands[index] = Command{Name: strings.ToUpper(name.String), Args: item.Array[1:]}
	}
	return commands, nil
}

func waitForReconnect(ctx context.Context) bool {
	timer := time.NewTimer(reconnectDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
