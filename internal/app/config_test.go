package app_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/app"
)

func TestLoadReturnsValidatedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := []byte(`
server: {addr: ":0", read_timeout: 1, write_timeout: 1}
replication: {role: "primary", backlog_size: 1024}
persistence:
  aof: {enabled: false, rewrite_interval: 60}
  rdb: {enabled: false}
datastructure:
  expiration: {max_sample_size: 1, max_sample_rounds: 1, check_interval: 1}
memory: {evict_strategy: ""}
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := app.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Address != ":0" {
		t.Fatalf("address = %q, want :0", cfg.Server.Address)
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("server: {addr: invalid}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := app.LoadConfig(path); err == nil {
		t.Fatal("Load should reject invalid config")
	}
}

func TestValidateAcceptsReplicaConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Role = "replica"
	cfg.Replication.PrimaryAddress = "127.0.0.1:6379"
	cfg.Replication.Username = "default"
	cfg.Replication.Password = "secret"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsReplicaWithoutPrimary(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Role = "replica"

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate should reject a replica without replication.primary_addr")
	}
}

func validConfig() app.Config {
	return app.Config{
		Server:      app.ServerConfig{Address: ":6379", ReadTimeout: 1, WriteTimeout: 1},
		Replication: app.ReplicationConfig{Role: "primary", BacklogSize: 1024},
		Persistence: app.PersistenceConfig{AOF: app.AOFConfig{RewriteInterval: 60}},
		Datastructure: app.DatastructureConfig{Expiration: app.ExpirationConfig{
			MaxSampleSize: 1, MaxSampleRounds: 1, CheckInterval: 1,
		}},
	}
}
