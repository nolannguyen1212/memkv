package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Configuration struct {
	Server        ServerConfiguration        `yaml:"server"`
	Replication   ReplicationConfiguration   `yaml:"replication"`
	Persistence   PersistenceConfiguration   `yaml:"persistence"`
	Datastructure DatastructureConfiguration `yaml:"datastructure"`
	Memory        MemoryConfiguration        `yaml:"memory"`
	Logging       LoggingConfiguration       `yaml:"logging"`
}

type ServerConfiguration struct {
	Address      string `yaml:"addr"`
	ReadTimeout  int    `yaml:"read_timeout"`
	WriteTimeout int    `yaml:"write_timeout"`
	Auth         string `yaml:"auth"`
}

type ReplicationConfiguration struct {
	BacklogSize       int `yaml:"backlog_size"`
	HeartbeatInterval int `yaml:"heartbeat_interval"`
	HeartbeatTimeout  int `yaml:"heartbeat_timeout"`
}

type PersistenceConfiguration struct {
	AOF AOFConfiguration `yaml:"aof"`
	RDB RDBConfiguration `yaml:"rdb"`
}

type AOFConfiguration struct {
	Enabled         bool   `yaml:"enabled"`
	Filename        string `yaml:"filename"`
	RewriteInterval int    `yaml:"rewrite_interval"`
	MaxSizeMB       int    `yaml:"max_size_mb"`
}

type RDBConfiguration struct {
	Enabled  bool   `yaml:"enabled"`
	Filename string `yaml:"filename"`
}

type DatastructureConfiguration struct {
	Expiration ExpirationConfiguration `yaml:"expiration"`
}

type ExpirationConfiguration struct {
	MaxSampleSize   int `yaml:"max_sample_size"`
	MaxSampleRounds int `yaml:"max_sample_rounds"`
	CheckInterval   int `yaml:"check_interval"`
}

type MemoryConfiguration struct {
	KeyLimit      *int   `yaml:"key_limit"`
	EvictStrategy string `yaml:"evict_strategy"`
}

type LoggingConfiguration struct {
	Level              string `yaml:"level"`
	VerbosePersistence bool   `yaml:"verbose_persistence"`
}

var Global *Configuration

func Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var configuration Configuration
	if err := yaml.Unmarshal(data, &configuration); err != nil {
		return err
	}

	Global = &configuration
	return nil
}

func (configuration *Configuration) ReadTimeout() time.Duration {
	return time.Duration(configuration.Server.ReadTimeout) * time.Second
}

func (configuration *Configuration) WriteTimeout() time.Duration {
	return time.Duration(configuration.Server.WriteTimeout) * time.Second
}

func (configuration *Configuration) AOFRewriteInterval() time.Duration {
	return time.Duration(configuration.Persistence.AOF.RewriteInterval) * time.Second
}

func (configuration *Configuration) ExpirationCheckInterval() time.Duration {
	return time.Duration(configuration.Datastructure.Expiration.CheckInterval) * time.Second
}

func (configuration *Configuration) HeartbeatInterval() time.Duration {
	return time.Duration(configuration.Replication.HeartbeatInterval) * time.Second
}

func (configuration *Configuration) HeartbeatTimeout() time.Duration {
	return time.Duration(configuration.Replication.HeartbeatTimeout) * time.Second
}
