package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/core"
	_ "github.com/william1nguyen/valkeydb/internal/core/replication"
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol"
	"github.com/william1nguyen/valkeydb/internal/replication"
	"github.com/william1nguyen/valkeydb/internal/server"
)

const defaultConfigPath = "config.yaml"

func main() {
	joinAddr := flag.String("join-addr", "", "Primary address to join as replica")
	joinAPIKey := flag.String("join-apikey", "", "API key of the primary to join")
	flag.Parse()

	if err := config.Load(defaultConfigPath); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	core.ConfigureSystem(
		config.Global.Server.Auth,
		config.Global.Persistence.AOF.Enabled,
		config.Global.Persistence.RDB.Enabled,
	)

	store := core.NewStore(core.StoreConfig{
		ExpirationCheckInterval: config.Global.ExpirationCheckInterval(),
		ExpirationMaxSampleSize: config.Global.Datastructure.Expiration.MaxSampleSize,
		ExpirationMaxRounds:     config.Global.Datastructure.Expiration.MaxSampleRounds,
		KeyLimit:                config.Global.Memory.KeyLimit,
		EvictStrategy:           config.Global.Memory.EvictStrategy,
	})

	aof, err := persistence.OpenAOF(
		config.Global.Persistence.AOF.Filename,
		config.Global.Persistence.AOF.Enabled,
	)
	if err != nil {
		log.Fatalf("Failed to open AOF: %v", err)
	}

	rdb, err := persistence.OpenRDB(
		config.Global.Persistence.RDB.Filename,
		config.Global.Persistence.RDB.Enabled,
	)
	if err != nil {
		log.Fatalf("Failed to open RDB: %v", err)
	}

	replicationManager := replication.NewManager(replication.ManagerConfig{
		BacklogCapacity:   config.Global.Replication.BacklogSize,
		HeartbeatInterval: config.Global.HeartbeatInterval(),
		HeartbeatTimeout:  config.Global.HeartbeatTimeout(),
		ListeningAddress:  config.Global.Server.Address,
	})

	ctx := core.NewContext(store, aof, replicationManager)

	replicationManager.SetReplaying(true)
	if err := loadRDBSnapshot(rdb, store); err != nil {
		log.Fatalf("Failed to restore RDB: %v", err)
	}
	if err := loadAOFCommands(aof, ctx); err != nil {
		log.Fatalf("Failed to replay AOF: %v", err)
	}
	replicationManager.SetReplaying(false)

	stopLiveness := make(chan struct{})
	go replicationManager.StartLivenessMonitor(stopLiveness)

	if *joinAddr != "" && *joinAPIKey != "" {
		dispatchContext := core.NewConnContext(ctx, nil)
		dispatch := func(cmd string, args []protocol.Value) {
			core.Execute(dispatchContext, cmd, args)
		}
		replicationManager.SetReplica(*joinAddr, *joinAPIKey, dispatch)
	}

	valkeyServer := server.New(config.Global.Server.Address, ctx, aof, rdb)

	log.Printf("Server API key: %s", replicationManager.ServerAPIKey())
	log.Printf("Starting ValkeyDB on %s", config.Global.Server.Address)
	if err := valkeyServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func loadRDBSnapshot(rdb *persistence.RDB, store *core.Store) error {
	snapshot, err := rdb.Load(config.Global.Persistence.RDB.Filename)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}
	for key, metadata := range snapshot.KeyspaceData {
		store.Keyspace.Restore(key, metadata)
	}

	for key, entry := range snapshot.DictData {
		if len(snapshot.KeyspaceData) == 0 {
			if !store.PrepareWrite(key, datastructure.KeyTypeString, false) {
				continue
			}
		} else if exists, valid := store.HasType(key, datastructure.KeyTypeString); !exists || !valid {
			continue
		}
		store.Dictionary.Set(key, entry.Value, 0)
		if len(snapshot.KeyspaceData) == 0 && !entry.ExpiredAt.IsZero() {
			store.Keyspace.ExpireAt(key, entry.ExpiredAt)
		}
	}

	for key, entry := range snapshot.SetData {
		if len(snapshot.KeyspaceData) == 0 {
			if !store.PrepareWrite(key, datastructure.KeyTypeSet, false) {
				continue
			}
		} else if exists, valid := store.HasType(key, datastructure.KeyTypeSet); !exists || !valid {
			continue
		}
		if len(entry.Members) == 0 {
			continue
		}
		members := make([]string, 0, len(entry.Members))
		for member := range entry.Members {
			members = append(members, member)
		}
		store.Set.Add(key, members...)
		if len(snapshot.KeyspaceData) == 0 && !entry.ExpiredAt.IsZero() {
			store.Keyspace.ExpireAt(key, entry.ExpiredAt)
		}
	}

	for key, values := range snapshot.ListData {
		if len(snapshot.KeyspaceData) == 0 {
			if !store.PrepareWrite(key, datastructure.KeyTypeList, false) {
				continue
			}
		} else if exists, valid := store.HasType(key, datastructure.KeyTypeList); !exists || !valid {
			continue
		}
		if len(values) > 0 {
			store.List.RightPush(key, values...)
		}
	}

	for key, hash := range snapshot.HashData {
		if len(snapshot.KeyspaceData) == 0 {
			if !store.PrepareWrite(key, datastructure.KeyTypeHash, false) {
				continue
			}
		} else if exists, valid := store.HasType(key, datastructure.KeyTypeHash); !exists || !valid {
			continue
		}
		for field, value := range hash {
			store.Hash.Set(key, field, value)
		}
	}

	for key, members := range snapshot.SortedSetData {
		if len(snapshot.KeyspaceData) == 0 {
			if !store.PrepareWrite(key, datastructure.KeyTypeSortedSet, false) {
				continue
			}
		} else if exists, valid := store.HasType(key, datastructure.KeyTypeSortedSet); !exists || !valid {
			continue
		}
		items := make([]datastructure.ScoreMember, 0, len(members))
		for member, score := range members {
			items = append(items, datastructure.ScoreMember{Member: member, Score: score})
		}
		store.SortedSet.Add(key, items...)
	}

	log.Printf("RDB loaded: %d string, %d set, %d list, %d hash, %d sorted-set keys",
		len(snapshot.DictData), len(snapshot.SetData), len(snapshot.ListData), len(snapshot.HashData), len(snapshot.SortedSetData))
	return nil
}

func loadAOFCommands(aof *persistence.AOF, ctx *core.Context) error {
	connContext := core.NewConnContext(ctx, nil)
	return aof.Load(config.Global.Persistence.AOF.Filename, func(cmd string, args []protocol.Value) error {
		result := core.Execute(connContext, cmd, args)
		if result.Type == protocol.TypeError {
			return fmt.Errorf("%s", result.String)
		}
		return nil
	})
}
