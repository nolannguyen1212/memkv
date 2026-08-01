package replication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type Role int32

const (
	RolePrimary Role = iota
	RoleReplica
)

type ReplicaConnection struct {
	ID                 string
	Address            string
	Connection         net.Conn
	AcknowledgedOffset atomic.Int64
	mutex              sync.Mutex
}

func (replicaConnection *ReplicaConnection) Send(data []byte) error {
	replicaConnection.mutex.Lock()
	defer replicaConnection.mutex.Unlock()
	return writeAll(replicaConnection.Connection, data)
}

type ManagerConfig struct {
	BacklogCapacity  int
	ListeningAddress string
}

type Manager struct {
	role              Role
	primaryStreamID   string
	primaryAddress    string
	primaryConnection net.Conn
	backlog           *Backlog
	replicas          []*ReplicaConnection
	replaying         atomic.Bool
	replicationOffset atomic.Int64
	stopReplication   chan struct{}
	config            ManagerConfig
	mutex             sync.RWMutex
	propagateMutex    sync.Mutex
	waitGroup         sync.WaitGroup
}

func (manager *Manager) Close() error {
	manager.mutex.Lock()
	if manager.stopReplication != nil {
		close(manager.stopReplication)
		manager.stopReplication = nil
	}
	primaryConnection := manager.primaryConnection
	manager.primaryConnection = nil
	replicas := append([]*ReplicaConnection(nil), manager.replicas...)
	manager.replicas = nil
	manager.mutex.Unlock()

	var firstErr error
	if primaryConnection != nil {
		firstErr = primaryConnection.Close()
	}
	for _, replica := range replicas {
		if err := replica.Connection.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	manager.waitGroup.Wait()
	return firstErr
}

func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		role:            RolePrimary,
		primaryStreamID: mustGenerateID(20),
		backlog:         NewBacklog(config.BacklogCapacity),
		config:          config,
	}
}

// mustGenerateID creates protocol identifiers, not credentials. Failure means
// the operating system's secure random source is unavailable, so startup
// cannot safely continue.
func mustGenerateID(length int) string {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(fmt.Sprintf("generate replication ID: %v", err))
	}
	return hex.EncodeToString(bytes)
}

func (manager *Manager) SetReplaying(v bool) {
	manager.replaying.Store(v)
}

func (manager *Manager) Propagate(data []byte) {
	if !manager.IsPrimary() || manager.replaying.Load() {
		return
	}
	manager.propagateMutex.Lock()
	defer manager.propagateMutex.Unlock()

	manager.backlog.Write(data)

	manager.mutex.RLock()
	snapshot := make([]*ReplicaConnection, len(manager.replicas))
	copy(snapshot, manager.replicas)
	manager.mutex.RUnlock()

	log.Printf("[PRIMARY] Propagate to %d replica(s): %s", len(snapshot), strings.TrimRight(string(data), "\r\n"))

	for _, replica := range snapshot {
		if err := replica.Send(data); err != nil {
			manager.RemoveReplica(replica.ID)
		}
	}
}

func (manager *Manager) AddReplica(replica *ReplicaConnection) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.replicas = append(manager.replicas, replica)
}

func (manager *Manager) RemoveReplica(id string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	for i, replica := range manager.replicas {
		if replica.ID == id {
			manager.replicas = append(manager.replicas[:i], manager.replicas[i+1:]...)
			return
		}
	}
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

func (manager *Manager) SetReplica(address, username, password string, dispatch func(string, []resp.Value)) {
	manager.mutex.Lock()
	if manager.stopReplication != nil {
		close(manager.stopReplication)
	}
	manager.stopReplication = make(chan struct{})
	if manager.primaryConnection != nil {
		_ = manager.primaryConnection.Close()
		manager.primaryConnection = nil
	}
	stopChan := manager.stopReplication
	manager.role = RoleReplica
	manager.primaryStreamID = ""
	manager.primaryAddress = address
	manager.mutex.Unlock()

	manager.waitGroup.Add(1)
	go func() {
		defer manager.waitGroup.Done()
		manager.reconnectLoop(stopChan, address, username, password, dispatch)
	}()
}

func (manager *Manager) Promote() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.stopReplication != nil {
		close(manager.stopReplication)
		manager.stopReplication = nil
	}
	if manager.primaryConnection != nil {
		_ = manager.primaryConnection.Close()
		manager.primaryConnection = nil
	}
	manager.role = RolePrimary
	manager.primaryAddress = ""
	manager.primaryStreamID = mustGenerateID(20)
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

func (manager *Manager) PrimaryAddress() string {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return manager.primaryAddress
}

func (manager *Manager) PrimaryStreamID() string {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	return manager.primaryStreamID
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
		manager.backlog.CurrentOffset(),
		len(manager.replicas),
		strings.Join(manager.activeReplicasLocked(), ","),
	)
}
