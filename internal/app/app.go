package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/engine"
	"github.com/william1nguyen/valkeydb/internal/replication"
	"github.com/william1nguyen/valkeydb/internal/resp"
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
		logger = slog.Default()
	}
	engine.ConfigureSystem(
		cfg.Server.Auth,
		cfg.Persistence.AOF.Enabled,
		cfg.Persistence.RDB.Enabled,
	)

	database := store.New(store.Config{
		ExpirationCheckInterval: cfg.ExpirationCheckInterval(),
		ExpirationMaxSampleSize: cfg.Datastructure.Expiration.MaxSampleSize,
		ExpirationMaxRounds:     cfg.Datastructure.Expiration.MaxSampleRounds,
		KeyLimit:                cfg.Memory.KeyLimit,
		EvictStrategy:           cfg.Memory.EvictStrategy,
	})

	log, err := wal.Open(cfg.Persistence.AOF.Filename, cfg.Persistence.AOF.Enabled)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}

	snapshotFile, err := snapshot.Open(cfg.Persistence.RDB.Filename, cfg.Persistence.RDB.Enabled)
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("open RDB: %w", err)
	}

	replicationManager := replication.NewManager(replication.ManagerConfig{
		BacklogCapacity:  cfg.Replication.BacklogSize,
		ListeningAddress: cfg.Server.Address,
	})
	executionContext := engine.NewContext(database, log, replicationManager)

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
		_ = a.closeResources()
		return nil, err
	}
	a.server = server.New(server.Config{
		Address:      cfg.Server.Address,
		Auth:         cfg.Server.Auth,
		ReadTimeout:  cfg.ReadTimeout(),
		WriteTimeout: cfg.WriteTimeout(),
	}, executionContext, logger)

	return a, nil
}

func (a *Application) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return a.closeResources()
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	a.configureReplica()

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- a.server.ListenAndServe()
	}()

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		a.runAOFRewrite(runContext)
	}()

	var runErr error
	serverStopped := false
	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		serverStopped = true
		if err != nil {
			runErr = fmt.Errorf("serve: %w", err)
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

	snapshotErr := a.saveSnapshot()
	resourceErr := a.closeResources()
	return errors.Join(runErr, serverErr, snapshotErr, resourceErr)
}

func (a *Application) recover() error {
	a.replication.SetReplaying(true)
	defer a.replication.SetReplaying(false)

	if err := a.loadSnapshot(); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	if err := a.replayAOF(); err != nil {
		return fmt.Errorf("replay AOF: %w", err)
	}
	return nil
}

func (a *Application) configureReplica() {
	if a.config.Replication.Role != "replica" {
		return
	}
	dispatchContext := engine.NewConnContext(a.engine, nil)
	dispatch := func(command string, args []resp.Value) {
		engine.Execute(dispatchContext, command, args)
	}
	a.replication.SetReplica(
		a.config.Replication.PrimaryAddress,
		a.config.Replication.Username,
		a.config.Replication.Password,
		dispatch,
	)
}

func (a *Application) runAOFRewrite(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	lastRewrite := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sizeMB := a.wal.SizeMB()
			sizeLimit := float64(a.config.Persistence.AOF.MaxSizeMB)
			sizeExceeded := sizeLimit > 0 && sizeMB >= sizeLimit
			timeExceeded := time.Since(lastRewrite) >= a.config.AOFRewriteInterval()
			if !sizeExceeded && !timeExceeded {
				continue
			}
			if err := a.rewriteAOF(); err != nil {
				a.logger.Error("rewrite AOF", "error", err)
				continue
			}
			lastRewrite = time.Now()
			a.logger.Info("AOF rewrite complete")
		}
	}
}

func (a *Application) rewriteAOF() error {
	a.store.ExecMu.Lock()
	defer a.store.ExecMu.Unlock()
	return a.wal.RewriteAll(
		a.store.Dictionary.Snapshot(),
		a.store.Set.Snapshot(),
		a.store.List.Snapshot(),
		a.store.Hash.Snapshot(),
		a.store.SortedSet.Snapshot(),
		a.store.Keyspace.Snapshot(),
		a.config.Persistence.AOF.Filename,
	)
}

func (a *Application) saveSnapshot() error {
	if !a.config.Persistence.RDB.Enabled {
		return nil
	}
	a.store.ExecMu.Lock()
	data := snapshot.Data{
		KeyspaceData:  a.store.Keyspace.Snapshot(),
		DictData:      a.store.Dictionary.Snapshot(),
		SetData:       a.store.Set.Snapshot(),
		ListData:      a.store.List.Snapshot(),
		HashData:      a.store.Hash.Snapshot(),
		SortedSetData: a.store.SortedSet.Snapshot(),
	}
	a.store.ExecMu.Unlock()
	if err := a.snapshot.Save(data, a.config.Persistence.RDB.Filename); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

func (a *Application) loadSnapshot() error {
	data, err := a.snapshot.Load(a.config.Persistence.RDB.Filename)
	if err != nil || data == nil {
		return err
	}
	for key, metadata := range data.KeyspaceData {
		a.store.Keyspace.Restore(key, metadata)
	}
	for key, entry := range data.DictData {
		if !a.prepareRestoredKey(key, store.KeyTypeString, data.KeyspaceData) {
			continue
		}
		a.store.Dictionary.Set(key, entry.Value, 0)
		if len(data.KeyspaceData) == 0 && !entry.ExpiredAt.IsZero() {
			a.store.Keyspace.ExpireAt(key, entry.ExpiredAt)
		}
	}
	for key, entry := range data.SetData {
		if !a.prepareRestoredKey(key, store.KeyTypeSet, data.KeyspaceData) || len(entry.Members) == 0 {
			continue
		}
		members := make([]string, 0, len(entry.Members))
		for member := range entry.Members {
			members = append(members, member)
		}
		a.store.Set.Add(key, members...)
		if len(data.KeyspaceData) == 0 && !entry.ExpiredAt.IsZero() {
			a.store.Keyspace.ExpireAt(key, entry.ExpiredAt)
		}
	}
	for key, values := range data.ListData {
		if a.prepareRestoredKey(key, store.KeyTypeList, data.KeyspaceData) && len(values) > 0 {
			a.store.List.RightPush(key, values...)
		}
	}
	for key, hash := range data.HashData {
		if !a.prepareRestoredKey(key, store.KeyTypeHash, data.KeyspaceData) {
			continue
		}
		for field, value := range hash {
			a.store.Hash.Set(key, field, value)
		}
	}
	for key, members := range data.SortedSetData {
		if !a.prepareRestoredKey(key, store.KeyTypeSortedSet, data.KeyspaceData) {
			continue
		}
		items := make([]store.ScoreMember, 0, len(members))
		for member, score := range members {
			items = append(items, store.ScoreMember{Member: member, Score: score})
		}
		a.store.SortedSet.Add(key, items...)
	}
	return nil
}

func (a *Application) prepareRestoredKey(key string, keyType store.KeyType, metadata map[string]store.KeyMetadata) bool {
	if len(metadata) == 0 {
		return a.store.PrepareWrite(key, keyType, false)
	}
	exists, valid := a.store.HasType(key, keyType)
	return exists && valid
}

func (a *Application) replayAOF() error {
	connection := engine.NewConnContext(a.engine, nil)
	return a.wal.Load(a.config.Persistence.AOF.Filename, func(command string, args []resp.Value) error {
		result := engine.Execute(connection, command, args)
		if result.Type == resp.TypeError {
			return fmt.Errorf("execute %s: %s", command, result.String)
		}
		return nil
	})
}

func (a *Application) closeResources() error {
	return errors.Join(
		a.replication.Close(),
		a.wal.Close(),
		a.snapshot.Close(),
	)
}
