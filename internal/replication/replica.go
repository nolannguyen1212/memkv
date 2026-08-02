package replication

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/william1nguyen/memkv/internal/mutation"
	"github.com/william1nguyen/memkv/internal/resp"
)

const reconnectDelay = 2 * time.Second

func (manager *Manager) connectToPrimary(
	ctx context.Context,
	config ReplicaConfig,
	apply ApplyBatch,
	restore ApplySnapshot,
) error {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", config.Address)

	if err != nil {
		return err
	}

	defer func() { _ = connection.Close() }()

	if !manager.setPrimaryConnection(ctx, connection) {
		return ctx.Err()
	}

	defer manager.clearPrimaryConnection(connection)

	session := newReplicaSession(connection)

	if err := manager.handshake(session, config.Username, config.Password); err != nil {
		return err
	}

	if err := manager.synchronize(session, restore); err != nil {
		return err
	}

	return manager.streamCommands(session, apply)
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

func (manager *Manager) streamCommands(session *replicaSession, apply ApplyBatch) error {
	for {
		value, err := resp.Decode(session.reader)

		if err != nil {
			return err
		}

		if value.Type != resp.TypeArray || len(value.Array) == 0 {
			return fmt.Errorf("replication stream contains a non-command value")
		}

		commands, err := decodeStreamBatch(value)

		if err != nil {
			return err
		}

		if err := apply(commands); err != nil {
			return fmt.Errorf("apply replication batch: %w", err)
		}

		manager.replicationOffset.Add(int64(len(resp.Encode(value))))
	}
}

func (manager *Manager) reconnectLoop(
	ctx context.Context,
	config ReplicaConfig,
	apply ApplyBatch,
	restore ApplySnapshot,
) {
	for {
		err := manager.connectToPrimary(ctx, config, apply, restore)

		if err != nil && ctx.Err() == nil {
			manager.logger.Warn("primary disconnected", "primary", config.Address, "error", err)
		}

		if !waitForReconnect(ctx) {
			return
		}
	}
}

func decodeStreamBatch(value resp.Value) (mutation.Batch, error) {
	if value.Array[0].Type == resp.TypeArray {
		return decodeBatch(value)
	}

	command, err := decodeCommand(value)

	if err != nil {
		return nil, err
	}

	return mutation.Batch{command}, nil
}

func decodeBatch(value resp.Value) ([]Command, error) {
	commands := make([]Command, len(value.Array))

	for index, item := range value.Array {
		command, err := decodeCommand(item)

		if err != nil {
			return nil, fmt.Errorf("replication batch command %d: %w", index, err)
		}

		commands[index] = command
	}

	return commands, nil
}

func decodeCommand(value resp.Value) (Command, error) {
	if value.Type != resp.TypeArray || len(value.Array) == 0 {
		return Command{}, fmt.Errorf("command is invalid")
	}

	name := value.Array[0]

	if name.Type != resp.TypeBulkString && name.Type != resp.TypeSimpleString {
		return Command{}, fmt.Errorf("command name is invalid")
	}

	args := make([]string, len(value.Array)-1)

	for index, argument := range value.Array[1:] {
		if argument.Type != resp.TypeBulkString && argument.Type != resp.TypeSimpleString {
			return Command{}, fmt.Errorf("command argument %d is invalid", index)
		}

		args[index] = argument.String
	}

	return mutation.New(strings.ToUpper(name.String), args...), nil
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
