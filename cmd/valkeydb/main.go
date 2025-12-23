package main

import (
	"log"

	"github.com/william1nguyen/valkeydb/command"
	_ "github.com/william1nguyen/valkeydb/command/hashcmd"
	_ "github.com/william1nguyen/valkeydb/command/listcmd"
	_ "github.com/william1nguyen/valkeydb/command/pubsubcmd"
	_ "github.com/william1nguyen/valkeydb/command/setcmd"
	_ "github.com/william1nguyen/valkeydb/command/stringcmd"
	"github.com/william1nguyen/valkeydb/command/systemcmd"
	"github.com/william1nguyen/valkeydb/config"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/persistence"
	"github.com/william1nguyen/valkeydb/protocol"
	"github.com/william1nguyen/valkeydb/server"
)

const defaultConfigPath = "config.yaml"

func main() {
	if err := config.Load(defaultConfigPath); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	systemcmd.Configure(
		config.Global.Server.Auth,
		config.Global.Persistence.AOF.Enabled,
		config.Global.Persistence.RDB.Enabled,
	)

	dataStore := store.New(store.Config{
		ExpirationCheckInterval: config.Global.ExpirationCheckInterval(),
		ExpirationMaxSampleSize: config.Global.Datastructure.Expiration.MaxSampleSize,
		ExpirationMaxRounds:     config.Global.Datastructure.Expiration.MaxSampleRounds,
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

	loadRDBSnapshot(rdb, dataStore)
	loadAOFCommands(aof, dataStore)

	valkeyServer := server.New(config.Global.Server.Address, dataStore, aof, rdb)

	log.Printf("Starting ValkeyDB on %s", config.Global.Server.Address)
	if err := valkeyServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func loadRDBSnapshot(rdb *persistence.RDB, dataStore *store.Store) {
	snapshot, err := rdb.Load(config.Global.Persistence.RDB.Filename)
	if err != nil {
		log.Printf("RDB load error: %v", err)
		return
	}

	if snapshot == nil {
		return
	}

	for key, entry := range snapshot.DictData {
		dataStore.Dictionary.Set(key, entry.Value, 0)
		if !entry.ExpiredAt.IsZero() {
			dataStore.Dictionary.ExpireAt(key, entry.ExpiredAt)
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
		dataStore.Set.Add(key, members...)
		if !entry.ExpiredAt.IsZero() {
			dataStore.Set.ExpireAt(key, entry.ExpiredAt)
		}
	}

	for key, values := range snapshot.ListData {
		if len(values) > 0 {
			dataStore.List.RightPush(key, values...)
		}
	}

	for key, hash := range snapshot.HashData {
		for field, value := range hash {
			dataStore.HashMap.Set(key, field, value)
		}
	}

	log.Printf("RDB loaded: %d dict, %d set, %d list, %d hash keys",
		len(snapshot.DictData), len(snapshot.SetData), len(snapshot.ListData), len(snapshot.HashData))
}

func loadAOFCommands(aof *persistence.AOF, dataStore *store.Store) {
	aof.Load(config.Global.Persistence.AOF.Filename, func(commandName string, arguments []protocol.Value) {
		command.Execute(dataStore, commandName, arguments)
	})
}
