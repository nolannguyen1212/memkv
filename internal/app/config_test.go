package app_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/william1nguyen/memkv/internal/app"
)

func TestLoadReturnsValidatedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := []byte(`
server: {addr: ":0", read_timeout: 1, write_timeout: 1}
replication: {role: "primary", backlog_size: 1024}
persistence:
  wal: {enabled: false, rewrite_interval: 60}
  snapshot: {enabled: false}
datastructure:
  expiration: {check_interval: 1}
memory: {}
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

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := []byte(`
server: {addr: ":0", read_timeout: 1, write_timeout: 1, write_timout: 1}
replication: {role: "primary", backlog_size: 1024}
persistence: {wal: {enabled: false}}
datastructure: {expiration: {check_interval: 1}}
`)

	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := app.LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() accepted an unknown field")
	}
}

func TestValidateDoesNotRequireRewritePolicyWhenWALIsDisabled(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence.WAL.RewriteInterval = 0

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestRESPConfigOverridesDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.Server.RESP = app.RESPConfig{MaxBulkLength: 1024, MaxArrayLength: 16, MaxDepth: 4, MaxLineLength: 128}
	limits := cfg.RESPLimits()

	if limits.MaxBulkLength != 1024 {
		t.Fatalf("max bulk length = %d, want 1024", limits.MaxBulkLength)
	}

	if limits.MaxArrayLength != 16 {
		t.Fatalf("max array length = %d, want 16", limits.MaxArrayLength)
	}

	if limits.MaxDepth != 4 {
		t.Fatalf("max depth = %d, want 4", limits.MaxDepth)
	}

	if limits.MaxLineLength != 128 {
		t.Fatalf("max line length = %d, want 128", limits.MaxLineLength)
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

func TestValidateRejectsInvalidMaxKeys(t *testing.T) {
	cfg := validConfig()
	maxKeys := 0
	cfg.Memory.MaxKeys = &maxKeys

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate should reject a non-positive memory.max_keys")
	}
}

func TestValidateRejectsReplicaPersistence(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Role = "replica"
	cfg.Replication.PrimaryAddress = "127.0.0.1:6379"
	cfg.Persistence.WAL.Enabled = true
	cfg.Persistence.WAL.Filename = "replica.wal"

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate should reject persistence on a replica")
	}
}

func TestValidateAcceptsSnapshotWithWALSuffixRecovery(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence.WAL.Enabled = true
	cfg.Persistence.WAL.Filename = "memkv.wal"
	cfg.Persistence.Snapshot.Enabled = true
	cfg.Persistence.Snapshot.Filename = "dump.mksp"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected WAL and snapshot together: %v", err)
	}
}

func validConfig() app.Config {
	return app.Config{
		Server:        app.ServerConfig{Address: ":6379", ReadTimeout: 1, WriteTimeout: 1},
		Replication:   app.ReplicationConfig{Role: "primary", BacklogSize: 1024},
		Persistence:   app.PersistenceConfig{WAL: app.WALConfig{RewriteInterval: 60}},
		Datastructure: app.DatastructureConfig{Expiration: app.ExpirationConfig{CheckInterval: 1}},
	}
}
