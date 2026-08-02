package engine

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

func registerSystemCommands(registry *Registry) {
	registry.Register("AUTH", handleAuth)
	registry.Register("INFO", handleInfo)
	registry.Register("KEYS", handleKeys)
}

func handleAuth(connContext *ConnContext, args []string) resp.Value {
	if len(args) != 1 && len(args) != 2 {
		return wrongArgCountError("auth")
	}
	password := args[len(args)-1]
	if len(args) == 2 && args[0] != "default" {
		return errorReply("ERR invalid username-password pair or user is disabled")
	}
	if password != connContext.system.Auth {
		return errorReply("ERR invalid password")
	}
	return okReply()
}

func handleInfo(connContext *ConnContext, args []string) resp.Value {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}
	return stringReply(buildInfo(connContext, section))
}

func buildInfo(connContext *ConnContext, section string) string {
	uptime := int(connContext.metrics.uptime().Seconds())
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	show := func(name string) bool {
		return section == "all" || section == name
	}

	var lines []string

	if show("server") {
		lines = append(lines, "uptime_in_seconds:"+strconv.Itoa(uptime))
	}
	if show("clients") {
		lines = append(lines, "connected_clients:"+strconv.FormatInt(connContext.metrics.openConnections.Load(), 10))
		lines = append(lines, "total_connections_received:"+strconv.FormatUint(connContext.metrics.totalConnections.Load(), 10))
	}
	if show("memory") {
		lines = append(lines, "used_memory:"+strconv.FormatUint(memStats.Alloc, 10))
	}
	if show("persistence") {
		lines = append(lines, "wal_enabled:"+boolStr(connContext.system.WALEnabled))
		lines = append(lines, "snapshot_enabled:"+boolStr(connContext.system.SnapshotEnabled))
	}
	if show("stats") {
		lines = append(lines, "total_commands_processed:"+strconv.FormatUint(connContext.metrics.totalCommands.Load(), 10))
	}
	if show("keyspace") {
		counts := make(map[store.KeyType]int)
		for _, metadata := range connContext.Store.Keyspace.Snapshot() {
			counts[metadata.Type]++
		}
		lines = append(lines, fmt.Sprintf("db0:string=%d,set=%d,list=%d,hash=%d,zset=%d",
			counts[store.KeyTypeString], counts[store.KeyTypeSet], counts[store.KeyTypeList],
			counts[store.KeyTypeHash], counts[store.KeyTypeSortedSet]))
	}
	if show("replication") && connContext.Replication != nil {
		lines = append(lines, connContext.Replication.Info())
	}

	return strings.Join(lines, "\n")
}

func handleKeys(connContext *ConnContext, args []string) resp.Value {
	if len(args) < 1 {
		return wrongArgCountError("keys")
	}

	pattern := args[0]
	var keys []string

	for key := range connContext.Store.Keyspace.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			keys = append(keys, key)
		}
	}

	return valuesToArray(keys)
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
