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

type LoggingConfiguration struct {
	Level              string `yaml:"level"`
	VerbosePersistence bool   `yaml:"verbose_persistence"`
}

var Global *Configuration

func Load(configPath string) error {
	fileContent, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	var configuration Configuration
	if err := yaml.Unmarshal(fileContent, &configuration); err != nil {
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
