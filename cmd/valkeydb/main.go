package main

import (
	"log"

	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/core"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol"
	"github.com/william1nguyen/valkeydb/internal/server"
)

const defaultConfigPath = "config.yaml"

func main() {
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

	ctx := core.NewContext(store, aof)

	loadRDBSnapshot(rdb, store)
	loadAOFCommands(aof, ctx)

	valkeyServer := server.New(config.Global.Server.Address, ctx, aof, rdb)

	log.Printf("Starting ValkeyDB on %s", config.Global.Server.Address)
	if err := valkeyServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func loadRDBSnapshot(rdb *persistence.RDB, store *core.Store) {
	snapshot, err := rdb.Load(config.Global.Persistence.RDB.Filename)
	if err != nil {
		log.Printf("RDB load error: %v", err)
		return
	}
	if snapshot == nil {
		return
	}

	for key, entry := range snapshot.DictData {
		store.Dictionary.Set(key, entry.Value, 0)
		if !entry.ExpiredAt.IsZero() {
			store.Dictionary.ExpireAt(key, entry.ExpiredAt)
		}
	}

	for key, entry := range snapshot.SetData {
		if len(entry.Members) == 0 {
			continue
		}
		members := make([]string, 0, len(entry.Members))
		for member := range entry.Members {
			members = append(members, member)
		}
		store.Set.Add(key, members...)
		if !entry.ExpiredAt.IsZero() {
			store.Set.ExpireAt(key, entry.ExpiredAt)
		}
	}

	for key, values := range snapshot.ListData {
		if len(values) > 0 {
			store.List.RightPush(key, values...)
		}
	}

	for key, hash := range snapshot.HashData {
		for field, value := range hash {
			store.Hash.Set(key, field, value)
		}
	}

	log.Printf("RDB loaded: %d dict, %d set, %d list, %d hash keys",
		len(snapshot.DictData), len(snapshot.SetData), len(snapshot.ListData), len(snapshot.HashData))
}

func loadAOFCommands(aof *persistence.AOF, ctx *core.Context) {
	connContext := core.NewConnContext(ctx)
	aof.Load(config.Global.Persistence.AOF.Filename, func(cmd string, args []protocol.Value) {
		core.Execute(connContext, cmd, args)
	})
}
