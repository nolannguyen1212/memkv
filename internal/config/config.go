package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Persistence   PersistenceConfig   `yaml:"persistence"`
	Datastructure DatastructureConfig `yaml:"datastructure"`
	Logging       LoggingConfig       `yaml:"logging"`
}

type ServerConfig struct {
	Addr         string `yaml:"addr"`
	ReadTimeout  int    `yaml:"read_timeout"`
	WriteTimeout int    `yaml:"write_timeout"`
}

type PersistenceConfig struct {
	AOF AOFConfig `yaml:"aof"`
	RDB RDBConfig `yaml:"rdb"`
}

type AOFConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Filename        string `yaml:"filename"`
	RewriteInterval int    `yaml:"rewrite_interval"`
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

type LoggingConfig struct {
	Level              string `yaml:"level"`
	VerbosePersistence bool   `yaml:"verbose_persistence"`
}

var Global *Config

func Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}

	Global = &cfg
	return nil
}

func (c *Config) GetReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeout) * time.Second
}

func (c *Config) GetWriteTimeout() time.Duration {
	return time.Duration(c.Server.WriteTimeout) * time.Second
}

func (c *Config) GetAOFRewriteInterval() time.Duration {
	return time.Duration(c.Persistence.AOF.RewriteInterval) * time.Second
}

func (c *Config) GetExpirationCheckInterval() time.Duration {
	return time.Duration(c.Datastructure.Expiration.CheckInterval) * time.Second
}
