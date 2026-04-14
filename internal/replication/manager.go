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
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
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
	_, err := replicaConnection.Connection.Write(data)
	return err
}

type ManagerConfig struct {
	BacklogCapacity   int
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	ListeningAddress  string
}

type Manager struct {
	serverAPIKey         string
	role                 Role
	primaryStreamID      string
	primaryAddress       string
	backlog              *Backlog
	replicas             []*ReplicaConnection
	replicaLastHeartbeat map[string]time.Time
	replaying            atomic.Bool
	stopReplication      chan struct{}
	config               ManagerConfig
	mutex                sync.RWMutex
}

func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		serverAPIKey:         generateHex(20),
		role:                 RolePrimary,
		primaryStreamID:      generateHex(20),
		backlog:              NewBacklog(config.BacklogCapacity),
		replicaLastHeartbeat: make(map[string]time.Time),
		config:               config,
	}
}

func generateHex(length int) string {
	bytes := make([]byte, length)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func (manager *Manager) ServerAPIKey() string {
	return manager.serverAPIKey
}

func (manager *Manager) ValidateAPIKey(key string) bool {
	return key == manager.serverAPIKey
}

func (manager *Manager) SetReplaying(v bool) {
	manager.replaying.Store(v)
}

func (manager *Manager) Propagate(data []byte) {
	if !manager.IsPrimary() || manager.replaying.Load() {
		return
	}

	manager.backlog.Write(data)

	manager.mutex.RLock()
	snapshot := make([]*ReplicaConnection, len(manager.replicas))
	copy(snapshot, manager.replicas)
	manager.mutex.RUnlock()

	log.Printf("[PRIMARY] Propagate to %d replica(s): %s", len(snapshot), strings.TrimRight(string(data), "\r\n"))

	for _, replica := range snapshot {
		go func(replicaConnection *ReplicaConnection) {
			if err := replicaConnection.Send(data); err != nil {
				manager.RemoveReplica(replicaConnection.ID)
			}
		}(replica)
	}
}

func (manager *Manager) AddReplica(replica *ReplicaConnection) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.replicas = append(manager.replicas, replica)
	manager.replicaLastHeartbeat[replica.ID] = time.Now()
}

func (manager *Manager) RemoveReplica(id string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	for i, replica := range manager.replicas {
		if replica.ID == id {
			manager.replicas = append(manager.replicas[:i], manager.replicas[i+1:]...)
			delete(manager.replicaLastHeartbeat, id)
			return
		}
	}
}

func (manager *Manager) UpdateAcknowledgedOffset(connection net.Conn, offset int64) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	for _, replica := range manager.replicas {
		if replica.Connection == connection {
			replica.AcknowledgedOffset.Store(offset)
			return
		}
	}
}

func (manager *Manager) UpdateReplicaHeartbeat(id string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.replicaLastHeartbeat[id] = time.Now()
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

func (manager *Manager) SetReplica(address, apiKey string, dispatch func(string, []protocol.Value)) {
	manager.mutex.Lock()
	if manager.stopReplication != nil {
		close(manager.stopReplication)
	}
	manager.stopReplication = make(chan struct{})
	stopChan := manager.stopReplication
	manager.role = RoleReplica
	manager.primaryStreamID = ""
	manager.primaryAddress = address
	manager.mutex.Unlock()

	go manager.reconnectLoop(stopChan, address, apiKey, dispatch)
}

func (manager *Manager) Promote() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.stopReplication != nil {
		close(manager.stopReplication)
		manager.stopReplication = nil
	}
	manager.role = RolePrimary
	manager.primaryAddress = ""
	manager.primaryStreamID = generateHex(20)
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
		"role:%s\nserver_api_key:%s\nreplication_id:%s\nreplication_offset:%d\nconnected_replicas:%d\nactive_replicas:%s",
		role,
		manager.serverAPIKey,
		manager.primaryStreamID,
		manager.backlog.CurrentOffset(),
		len(manager.replicas),
		strings.Join(manager.activeReplicasLocked(), ","),
	)
}
