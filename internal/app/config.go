package app

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/william1nguyen/memkv/internal/resp"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Replication   ReplicationConfig   `yaml:"replication"`
	Persistence   PersistenceConfig   `yaml:"persistence"`
	Datastructure DatastructureConfig `yaml:"datastructure"`
	Memory        MemoryConfig        `yaml:"memory"`
}

type ServerConfig struct {
	Address      string     `yaml:"addr"`
	ReadTimeout  int        `yaml:"read_timeout"`
	WriteTimeout int        `yaml:"write_timeout"`
	Auth         string     `yaml:"auth"`
	RESP         RESPConfig `yaml:"resp"`
}

type RESPConfig struct {
	MaxBulkLength  int `yaml:"max_bulk_length"`
	MaxArrayLength int `yaml:"max_array_length"`
	MaxDepth       int `yaml:"max_depth"`
	MaxLineLength  int `yaml:"max_line_length"`
}

type ReplicationConfig struct {
	Role           string `yaml:"role"`
	PrimaryAddress string `yaml:"primary_addr"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	BacklogSize    int    `yaml:"backlog_size"`
}

type PersistenceConfig struct {
	WAL      WALConfig      `yaml:"wal"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
}

type WALConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Filename        string `yaml:"filename"`
	RewriteInterval int    `yaml:"rewrite_interval"`
	MaxSizeMB       int    `yaml:"max_size_mb"`
}

type SnapshotConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Filename string `yaml:"filename"`
}

type DatastructureConfig struct {
	Expiration ExpirationConfig `yaml:"expiration"`
}

type ExpirationConfig struct {
	CheckInterval int `yaml:"check_interval"`
}

type MemoryConfig struct {
	MaxKeys *int `yaml:"max_keys"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)

	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Server.Address); err != nil {
		return fmt.Errorf("server.addr %q: %w", c.Server.Address, err)
	}

	if c.Server.ReadTimeout <= 0 {
		return fmt.Errorf("server.read_timeout must be positive")
	}

	if c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("server.write_timeout must be positive")
	}

	if !validRESPLimits(c.RESPLimits()) {
		return fmt.Errorf("server.resp limits must be positive")
	}

	if c.Replication.BacklogSize <= 0 {
		return fmt.Errorf("replication.backlog_size must be positive")
	}

	switch c.Replication.Role {
	case "primary":
		if c.Replication.PrimaryAddress != "" {
			return fmt.Errorf("replication.primary_addr must be empty for a primary")
		}
	case "replica":
		if _, _, err := net.SplitHostPort(c.Replication.PrimaryAddress); err != nil {
			return fmt.Errorf("replication.primary_addr %q: %w", c.Replication.PrimaryAddress, err)
		}

		if invalidReplicationUsername(c.Replication.Username) {
			return fmt.Errorf("replication.username must be empty or %q until ACL users are supported", "default")
		}

		if replicaPersistenceEnabled(c.Persistence) {
			return fmt.Errorf("persistence must be disabled on a replica")
		}
	default:
		return fmt.Errorf("replication.role must be %q or %q", "primary", "replica")
	}

	if c.Datastructure.Expiration.CheckInterval <= 0 {
		return fmt.Errorf("datastructure.expiration.check_interval must be positive")
	}

	if c.Persistence.WAL.Enabled {
		if strings.TrimSpace(c.Persistence.WAL.Filename) == "" {
			return fmt.Errorf("persistence.wal.filename is required when WAL is enabled")
		}

		if c.Persistence.WAL.RewriteInterval <= 0 {
			return fmt.Errorf("persistence.wal.rewrite_interval must be positive when WAL is enabled")
		}

		if c.Persistence.WAL.MaxSizeMB < 0 {
			return fmt.Errorf("persistence.wal.max_size_mb cannot be negative")
		}
	}

	if snapshotFilenameMissing(c.Persistence.Snapshot) {
		return fmt.Errorf("persistence.snapshot.filename is required when snapshot is enabled")
	}

	if invalidMaxKeys(c.Memory.MaxKeys) {
		return fmt.Errorf("memory.max_keys must be positive when set")
	}

	return nil
}

func validRESPLimits(limits resp.Limits) bool {
	if limits.MaxBulkLength <= 0 {
		return false
	}

	if limits.MaxArrayLength <= 0 {
		return false
	}

	if limits.MaxDepth <= 0 {
		return false
	}

	return limits.MaxLineLength > 0
}

func invalidReplicationUsername(username string) bool {
	if username == "" {
		return false
	}

	return username != "default"
}

func replicaPersistenceEnabled(config PersistenceConfig) bool {
	if config.WAL.Enabled {
		return true
	}

	return config.Snapshot.Enabled
}

func snapshotFilenameMissing(config SnapshotConfig) bool {
	if !config.Enabled {
		return false
	}

	return strings.TrimSpace(config.Filename) == ""
}

func invalidMaxKeys(maxKeys *int) bool {
	if maxKeys == nil {
		return false
	}

	return *maxKeys <= 0
}

func (c Config) RESPLimits() resp.Limits {
	limits := resp.DefaultLimits()

	if c.Server.RESP.MaxBulkLength != 0 {
		limits.MaxBulkLength = c.Server.RESP.MaxBulkLength
	}

	if c.Server.RESP.MaxArrayLength != 0 {
		limits.MaxArrayLength = c.Server.RESP.MaxArrayLength
	}

	if c.Server.RESP.MaxDepth != 0 {
		limits.MaxDepth = c.Server.RESP.MaxDepth
	}

	if c.Server.RESP.MaxLineLength != 0 {
		limits.MaxLineLength = c.Server.RESP.MaxLineLength
	}

	return limits
}

func (c Config) ReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeout) * time.Second
}

func (c Config) WriteTimeout() time.Duration {
	return time.Duration(c.Server.WriteTimeout) * time.Second
}

func (c Config) WALRewriteInterval() time.Duration {
	return time.Duration(c.Persistence.WAL.RewriteInterval) * time.Second
}

func (c Config) ExpirationCheckInterval() time.Duration {
	return time.Duration(c.Datastructure.Expiration.CheckInterval) * time.Second
}
