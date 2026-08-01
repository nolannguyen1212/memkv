package app

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Replication   ReplicationConfig   `yaml:"replication"`
	Persistence   PersistenceConfig   `yaml:"persistence"`
	Datastructure DatastructureConfig `yaml:"datastructure"`
	Memory        MemoryConfig        `yaml:"memory"`
	Logging       LoggingConfig       `yaml:"logging"`
}

type ServerConfig struct {
	Address      string `yaml:"addr"`
	ReadTimeout  int    `yaml:"read_timeout"`
	WriteTimeout int    `yaml:"write_timeout"`
	Auth         string `yaml:"auth"`
}

type ReplicationConfig struct {
	Role           string `yaml:"role"`
	PrimaryAddress string `yaml:"primary_addr"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	BacklogSize    int    `yaml:"backlog_size"`
}

type PersistenceConfig struct {
	AOF AOFConfig `yaml:"aof"`
	RDB RDBConfig `yaml:"rdb"`
}

type AOFConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Filename        string `yaml:"filename"`
	RewriteInterval int    `yaml:"rewrite_interval"`
	MaxSizeMB       int    `yaml:"max_size_mb"`
}

type RDBConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Filename string `yaml:"filename"`
}

type DatastructureConfig struct {
	Expiration ExpirationConfig `yaml:"expiration"`
}

type ExpirationConfig struct {
	MaxSampleSize   int `yaml:"max_sample_size"`
	MaxSampleRounds int `yaml:"max_sample_rounds"`
	CheckInterval   int `yaml:"check_interval"`
}

type MemoryConfig struct {
	KeyLimit      *int   `yaml:"key_limit"`
	EvictStrategy string `yaml:"evict_strategy"`
}

type LoggingConfig struct {
	Level              string `yaml:"level"`
	VerbosePersistence bool   `yaml:"verbose_persistence"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
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
	if c.Server.ReadTimeout <= 0 || c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("server timeouts must be positive")
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
		if c.Replication.Username != "" && c.Replication.Username != "default" {
			return fmt.Errorf("replication.username must be empty or %q until ACL users are supported", "default")
		}
	default:
		return fmt.Errorf("replication.role must be %q or %q", "primary", "replica")
	}
	if c.Datastructure.Expiration.CheckInterval <= 0 || c.Datastructure.Expiration.MaxSampleSize <= 0 || c.Datastructure.Expiration.MaxSampleRounds <= 0 {
		return fmt.Errorf("store.expiration values must be positive")
	}
	if c.Persistence.AOF.Enabled && strings.TrimSpace(c.Persistence.AOF.Filename) == "" {
		return fmt.Errorf("persistence.aof.filename is required when AOF is enabled")
	}
	if c.Persistence.RDB.Enabled && strings.TrimSpace(c.Persistence.RDB.Filename) == "" {
		return fmt.Errorf("persistence.rdb.filename is required when RDB is enabled")
	}
	if c.Persistence.AOF.RewriteInterval <= 0 {
		return fmt.Errorf("persistence.aof.rewrite_interval must be positive")
	}
	if c.Memory.KeyLimit != nil && *c.Memory.KeyLimit <= 0 {
		return fmt.Errorf("memory.key_limit must be positive when set")
	}
	switch c.Memory.EvictStrategy {
	case "", "lru", "lfu", "evict_first":
	default:
		return fmt.Errorf("memory.evict_strategy %q is not supported", c.Memory.EvictStrategy)
	}
	return nil
}

func (c Config) ReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeout) * time.Second
}

func (c Config) WriteTimeout() time.Duration {
	return time.Duration(c.Server.WriteTimeout) * time.Second
}

func (c Config) AOFRewriteInterval() time.Duration {
	return time.Duration(c.Persistence.AOF.RewriteInterval) * time.Second
}

func (c Config) ExpirationCheckInterval() time.Duration {
	return time.Duration(c.Datastructure.Expiration.CheckInterval) * time.Second
}
