package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Configuration struct {
	Server        ServerConfiguration        `yaml:"server"`
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

type PersistenceConfiguration struct {
	AOF AOFConfiguration `yaml:"aof"`
	RDB RDBConfiguration `yaml:"rdb"`
}

type AOFConfiguration struct {
	Enabled         bool   `yaml:"enabled"`
	Filename        string `yaml:"filename"`
	RewriteInterval int    `yaml:"rewrite_interval"`
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

	var cfg Configuration
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}

	Global = &cfg
	return nil
}

func (c *Configuration) ReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeout) * time.Second
}

func (c *Configuration) WriteTimeout() time.Duration {
	return time.Duration(c.Server.WriteTimeout) * time.Second
}

func (c *Configuration) AOFRewriteInterval() time.Duration {
	return time.Duration(c.Persistence.AOF.RewriteInterval) * time.Second
}

func (c *Configuration) ExpirationCheckInterval() time.Duration {
	return time.Duration(c.Datastructure.Expiration.CheckInterval) * time.Second
}
