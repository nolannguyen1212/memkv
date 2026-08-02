package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/app"
)

func TestApplicationStopsWithCanceledContext(t *testing.T) {
	cfg := app.Config{
		Server: app.ServerConfig{
			Address:      ":0",
			ReadTimeout:  1,
			WriteTimeout: 1,
		},
		Replication: app.ReplicationConfig{
			Role:        "primary",
			BacklogSize: 1024,
		},
		Persistence: app.PersistenceConfig{
			WAL: app.WALConfig{RewriteInterval: 60},
		},
		Datastructure: app.DatastructureConfig{
			Expiration: app.ExpirationConfig{
				CheckInterval: 1,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	application, err := app.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := application.Run(ctx); err != nil {
		t.Fatal(err)
	}
}
