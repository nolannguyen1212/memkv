package replication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/william1nguyen/valkeydb/internal/mutation"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Role int32

const (
	RolePrimary Role = iota
	RoleReplica
	defaultReplicaQueueCapacity = 256
)

type ApplyCommand func(mutation.Command) error
type ApplySnapshot func(store.LogicalSnapshot) error
type ApplyBatch func(mutation.Batch) error

type Command = mutation.Command

type replicaConnection struct {
	Address    string
	Connection net.Conn
	queue      chan []byte
	done       chan struct{}
	closeOnce  sync.Once
}

func newReplicaConnection(address string, connection net.Conn) *replicaConnection {
	return &replicaConnection{
		Address:    address,
		Connection: connection,
		queue:      make(chan []byte, defaultReplicaQueueCapacity),
		done:       make(chan struct{}),
	}
}

func (replica *replicaConnection) enqueue(data []byte) bool {
	copyOfData := append([]byte(nil), data...)
	select {
	case replica.queue <- copyOfData:
		return true
	case <-replica.done:
		return false
	default:
		return false
	}

}

func (replica *replicaConnection) close() {
	replica.closeOnce.Do(func() {
		close(replica.done)
		_ = replica.Connection.Close()
	})
}

type ManagerConfig struct {
	BacklogCapacity  int
	ListeningAddress string
	Logger           *slog.Logger
}

type Manager struct {
	role              Role
	primaryStreamID   string
	primaryConnection net.Conn
	listeningAddress  string
	backlog           *Backlog
	replicas          []*replicaConnection
	replaying         atomic.Bool
	replicationOffset atomic.Int64
	replicaCancel     context.CancelFunc
	mutex             sync.RWMutex
	propagateMutex    sync.Mutex
	waitGroup         sync.WaitGroup
	logger            *slog.Logger
}

func NewManager(config ManagerConfig) *Manager {
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Manager{
		role:             RolePrimary,
		primaryStreamID:  replicationID(),
		listeningAddress: config.ListeningAddress,
		backlog:          NewBacklog(config.BacklogCapacity),
		logger:           logger,
	}
}

func replicationID() string {
	value := make([]byte, 20)
	if _, err := rand.Read(value); err == nil {
		return hex.EncodeToString(value)
	}
	return hex.EncodeToString([]byte(fmt.Sprintf("%020d", time.Now().UnixNano())))
}

func (manager *Manager) Close() error {
	manager.mutex.Lock()
	if manager.replicaCancel != nil {
		manager.replicaCancel()
		manager.replicaCancel = nil
	}
	primaryConnection := manager.primaryConnection
	manager.primaryConnection = nil
	replicas := append([]*replicaConnection(nil), manager.replicas...)
	manager.replicas = nil
	manager.mutex.Unlock()

	var firstErr error
	if primaryConnection != nil {
		firstErr = primaryConnection.Close()
	}
	for _, replica := range replicas {
		replica.close()
	}
	manager.waitGroup.Wait()
	return firstErr
}

func (manager *Manager) SetReplaying(replaying bool) {
	manager.replaying.Store(replaying)
}

func (manager *Manager) Propagate(batch mutation.Batch) {
	manager.propagate(encodeMutationBatch(batch))
}

func (manager *Manager) propagate(data []byte) {
	if !manager.IsPrimary() || manager.replaying.Load() {
		return
	}

	manager.propagateMutex.Lock()
	defer manager.propagateMutex.Unlock()

	manager.backlog.Write(data)
	replicas := manager.replicaSnapshot()

	manager.logger.Debug("replication propagated", "replicas", len(replicas), "bytes", len(data))
	for _, replica := range replicas {
		if !replica.enqueue(data) {
			manager.removeReplica(replica)
		}
	}
}

func encodeMutationBatch(batch mutation.Batch) []byte {
	values := make([]resp.Value, len(batch))
	for index, command := range batch {
		items := make([]resp.Value, len(command.Args)+1)
		items[0] = resp.Value{Type: resp.TypeBulkString, String: command.Name}
		for argumentIndex, argument := range command.Args {
			items[argumentIndex+1] = resp.Value{Type: resp.TypeBulkString, String: argument}
		}
		values[index] = resp.Value{Type: resp.TypeArray, Array: items}
	}
	if len(values) == 1 {
		return []byte(resp.Encode(values[0]))
	}
	return []byte(resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
}

func (manager *Manager) replicaSnapshot() []*replicaConnection {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return append([]*replicaConnection(nil), manager.replicas...)
}

func (manager *Manager) addReplica(replica *replicaConnection) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.replicas = append(manager.replicas, replica)
}

func (manager *Manager) removeReplica(target *replicaConnection) {
	manager.mutex.Lock()
	for index, replica := range manager.replicas {
		if replica == target {
			manager.replicas = append(manager.replicas[:index], manager.replicas[index+1:]...)
			break
		}
	}
	manager.mutex.Unlock()
	target.close()
}

func (manager *Manager) ActiveReplicas() []string {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return manager.activeReplicasLocked()
}

func (manager *Manager) activeReplicasLocked() []string {
	addresses := make([]string, 0, len(manager.replicas))
	for _, replica := range manager.replicas {
		if replica.Address != "" {
			addresses = append(addresses, replica.Address)
		}
	}
	return addresses
}

func (manager *Manager) StartReplica(parent context.Context, address, username, password string, apply ApplyCommand, applyBatch ApplyBatch, restore ApplySnapshot) {
	ctx, cancel := context.WithCancel(parent)

	manager.mutex.Lock()
	manager.role = RoleReplica
	manager.primaryStreamID = ""
	manager.replicaCancel = cancel
	manager.mutex.Unlock()

	manager.waitGroup.Go(func() {
		manager.reconnectLoop(ctx, address, username, password, apply, applyBatch, restore)
	})
}

func (manager *Manager) IsPrimary() bool {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return manager.role == RolePrimary
}

func (manager *Manager) IsReplica() bool {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return manager.role == RoleReplica
}

func (manager *Manager) Info() string {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	role := "primary"
	if manager.role == RoleReplica {
		role = "replica"
	}

	return fmt.Sprintf(
		"role:%s\nreplication_id:%s\nreplication_offset:%d\nconnected_replicas:%d\nactive_replicas:%s",
		role,
		manager.primaryStreamID,
		manager.currentOffsetLocked(),
		len(manager.replicas),
		strings.Join(manager.activeReplicasLocked(), ","),
	)
}

func (manager *Manager) currentOffsetLocked() int64 {
	if manager.role == RoleReplica {
		return manager.replicationOffset.Load()
	}
	return manager.backlog.CurrentOffset()
}
