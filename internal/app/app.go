package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/engine"
	"github.com/william1nguyen/valkeydb/internal/mutation"
	"github.com/william1nguyen/valkeydb/internal/replication"
	"github.com/william1nguyen/valkeydb/internal/server"
	"github.com/william1nguyen/valkeydb/internal/snapshot"
	"github.com/william1nguyen/valkeydb/internal/store"
	"github.com/william1nguyen/valkeydb/internal/wal"
)

const shutdownTimeout = 10 * time.Second

type Application struct {
	config      Config
	logger      *slog.Logger
	store       *store.Store
	engine      *engine.Context
	wal         *wal.Log
	snapshot    *snapshot.File
	replication *replication.Manager
	server      *server.Server
}

func New(cfg Config, logger *slog.Logger) (*Application, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	database := store.New(store.Config{
		ExpirationCheckInterval: cfg.ExpirationCheckInterval(),
		MaxKeys:                 cfg.Memory.MaxKeys,
	})

	log, err := wal.Open(cfg.Persistence.WAL.Filename, cfg.Persistence.WAL.Enabled)

	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	snapshotFile := snapshot.New(cfg.Persistence.Snapshot.Enabled)

	replicationManager := replication.NewManager(replication.ManagerConfig{
		BacklogCapacity:  cfg.Replication.BacklogSize,
		ListeningAddress: cfg.Server.Address,
		Logger:           logger,
	})

	executionContext := engine.NewContext(database, log, replicationManager, engine.SystemConfig{
		Auth:            cfg.Server.Auth,
		WALEnabled:      cfg.Persistence.WAL.Enabled,
		SnapshotEnabled: cfg.Persistence.Snapshot.Enabled,
	})

	a := &Application{
		config:      cfg,
		logger:      logger,
		store:       database,
		engine:      executionContext,
		wal:         log,
		snapshot:    snapshotFile,
		replication: replicationManager,
	}

	if err := a.recover(); err != nil {
		return nil, errors.Join(err, a.closeResources())
	}

	a.server = server.New(server.Config{
		Address:      cfg.Server.Address,
		Auth:         cfg.Server.Auth,
		ReadTimeout:  cfg.ReadTimeout(),
		WriteTimeout: cfg.WriteTimeout(),
		RESPLimits:   cfg.RESPLimits(),
	}, executionContext, logger)

	return a, nil
}

func (a *Application) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return a.closeResources()
	}

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()

	engineContext, stopEngine := context.WithCancel(context.Background())
	defer stopEngine()

	engineErrors := make(chan error, 1)
	go func() {
		engineErrors <- a.engine.Run(engineContext)
	}()

	<-a.engine.Ready()
	a.startReplica(runContext)

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- a.server.ListenAndServe()
	}()

	var workers sync.WaitGroup
	workers.Go(func() {
		a.runWALRewrite(runContext)
	})

	var runErr error
	serverStopped := false
	engineStopped := false

	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		serverStopped = true

		if err != nil {
			runErr = fmt.Errorf("serve: %w", err)
		}

	case err := <-engineErrors:
		engineStopped = true

		if err != nil {
			runErr = fmt.Errorf("engine: %w", err)
		}
	}

	cancel()

	shutdownContext, stopShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer stopShutdown()
	serverErr := a.server.Close(shutdownContext)

	if !serverStopped {
		select {
		case err := <-serverErrors:
			if err != nil {
				runErr = fmt.Errorf("serve: %w", err)
			}

		case <-shutdownContext.Done():
			runErr = errors.Join(runErr, fmt.Errorf("wait for server: %w", shutdownContext.Err()))
		}
	}

	workers.Wait()

	var snapshotErr error

	if !engineStopped {
		snapshotErr = a.saveSnapshot()
	}

	stopEngine()

	if !engineStopped {
		if err := <-engineErrors; err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("engine: %w", err))
		}
	}

	resourceErr := a.closeResources()
	return errors.Join(runErr, serverErr, snapshotErr, resourceErr)
}

func (a *Application) recover() error {
	a.replication.SetReplaying(true)
	defer a.replication.SetReplaying(false)

	includedOffset, err := a.loadSnapshot()

	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	if a.config.Persistence.WAL.Enabled {
		if err := a.replayWAL(includedOffset); err != nil {
			return fmt.Errorf("replay WAL: %w", err)
		}
	}

	return nil
}

func (a *Application) startReplica(ctx context.Context) {
	if a.config.Replication.Role != "replica" {
		return
	}

	apply := func(commands mutation.Batch) error {
		batch := make([]engine.QueuedCommand, len(commands))
		copy(batch, commands)
		return a.engine.ApplyBatch(batch)
	}

	a.replication.StartReplica(
		ctx,
		replication.ReplicaConfig{
			Address:  a.config.Replication.PrimaryAddress,
			Username: a.config.Replication.Username,
			Password: a.config.Replication.Password,
		},
		apply,
		a.engine.RestoreSnapshot,
	)
}

func (a *Application) runWALRewrite(ctx context.Context) {
	if !a.config.Persistence.WAL.Enabled || a.config.Persistence.Snapshot.Enabled {
		return
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	lastRewrite := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sizeMB := a.wal.SizeMB()
			sizeLimit := float64(a.config.Persistence.WAL.MaxSizeMB)
			sizeExceeded := sizeLimit > 0 && sizeMB >= sizeLimit
			timeExceeded := time.Since(lastRewrite) >= a.config.WALRewriteInterval()

			if !sizeExceeded && !timeExceeded {
				continue
			}

			if err := a.rewriteWAL(); err != nil {
				a.logger.Error("rewrite WAL", "error", err)
				continue
			}

			lastRewrite = time.Now()
			a.logger.Info("WAL rewrite complete")
		}
	}
}

func (a *Application) rewriteWAL() error {
	return a.engine.RewriteWAL(a.config.Persistence.WAL.Filename)
}

func (a *Application) saveSnapshot() error {
	if !a.config.Persistence.Snapshot.Enabled {
		return nil
	}

	checkpoint, err := a.engine.Checkpoint()

	if err != nil {
		return err
	}

	data := snapshot.Data{State: checkpoint.State, IncludedOffset: checkpoint.WALOffset}

	if err := a.snapshot.Save(data, a.config.Persistence.Snapshot.Filename); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	return nil
}

func (a *Application) loadSnapshot() (int64, error) {
	data, err := a.snapshot.Load(a.config.Persistence.Snapshot.Filename)

	if err != nil || data == nil {
		return 0, err
	}

	if err := a.store.Restore(data.State, a.store.Now()); err != nil {
		return 0, err
	}

	return data.IncludedOffset, nil
}

func (a *Application) replayWAL(startOffset int64) error {
	if err := a.wal.RepairTail(); err != nil {
		return fmt.Errorf("repair WAL tail: %w", err)
	}

	apply := func(commands []wal.Command) error {
		batch := make([]engine.QueuedCommand, len(commands))
		copy(batch, commands)
		return a.engine.Replay(batch)
	}

	return a.wal.LoadBatchesFrom(
		a.config.Persistence.WAL.Filename,
		startOffset,
		apply,
	)
}

func (a *Application) closeResources() error {
	return errors.Join(
		a.replication.Close(),
		a.wal.Close(),
		a.snapshot.Close(),
	)
}
