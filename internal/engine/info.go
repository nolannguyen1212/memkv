package engine

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

const monitorBuffer = 128

var (
	serverStartTime          = time.Now()
	backgroundSaveInProgress int32
	statsMutex               sync.Mutex
	totalCommands            uint64
	totalConnections         uint64
	currentConnections       int64
	monitorMutex             sync.RWMutex
	monitorSubs              = map[chan resp.Value]struct{}{}
	configuredAuth           string
	aofEnabled               bool
	rdbEnabled               bool
)

func registerSystemCommands(registry *Registry) {
	registry.Register("AUTH", handleAuth)
	registry.Register("INFO", handleInfo)
	registry.Register("BGSAVE", handleBgsave)
	registry.Register("KEYS", handleKeys)
	registry.Register("MONITOR", handleMonitor)
}

func ConfigureSystem(auth string, aof, rdb bool) {
	configuredAuth = auth
	aofEnabled = aof
	rdbEnabled = rdb
}

func handleAuth(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 && len(args) != 2 {
		return wrongArgCountError("auth")
	}
	password := args[len(args)-1].String
	if len(args) == 2 && args[0].String != "default" {
		return errorReply("ERR invalid username-password pair or user is disabled")
	}
	if password != configuredAuth {
		return errorReply("ERR invalid password")
	}
	return okReply()
}

func handleInfo(connContext *ConnContext, args []resp.Value) resp.Value {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0].String)
	}
	return stringReply(buildInfo(connContext, section))
}

func buildInfo(connContext *ConnContext, section string) string {
	uptime := int(time.Since(serverStartTime).Seconds())
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
		lines = append(lines, "connected_clients:"+strconv.Itoa(int(GetCurrentConnections())))
		lines = append(lines, "total_connections_received:"+strconv.FormatUint(GetTotalConnections(), 10))
	}
	if show("memory") {
		lines = append(lines, "used_memory:"+strconv.FormatUint(memStats.Alloc, 10))
	}
	if show("persistence") {
		lines = append(lines, "aof_enabled:"+boolStr(aofEnabled))
		lines = append(lines, "rdb_enabled:"+boolStr(rdbEnabled))
		lines = append(lines, "bgsave_in_progress:"+strconv.Itoa(int(atomic.LoadInt32(&backgroundSaveInProgress))))
	}
	if show("stats") {
		lines = append(lines, "total_commands_processed:"+strconv.FormatUint(GetTotalCommands(), 10))
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

func handleBgsave(connContext *ConnContext, args []resp.Value) resp.Value {
	go func() {
		atomic.StoreInt32(&backgroundSaveInProgress, 1)
		defer atomic.StoreInt32(&backgroundSaveInProgress, 0)
	}()
	return resp.Value{Type: resp.TypeSimpleString, String: "Background saving started"}
}

func handleKeys(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgCountError("keys")
	}

	pattern := args[0].String
	var keys []string

	for key := range connContext.Store.Keyspace.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			keys = append(keys, key)
		}
	}

	return valuesToArray(keys)
}

func handleMonitor(connContext *ConnContext, args []resp.Value) resp.Value {
	return okReply()
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func GetTotalCommands() uint64 {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	return totalCommands
}

func IncCommands() {
	statsMutex.Lock()
	totalCommands++
	statsMutex.Unlock()
}

func GetTotalConnections() uint64 {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	return totalConnections
}

func GetCurrentConnections() int64 {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	return currentConnections
}

func IncConnections() {
	statsMutex.Lock()
	totalConnections++
	currentConnections++
	statsMutex.Unlock()
}

func DecConnections() {
	statsMutex.Lock()
	if currentConnections > 0 {
		currentConnections--
	}
	statsMutex.Unlock()
}

func MonitorSubscribe() chan resp.Value {
	monitorChannel := make(chan resp.Value, monitorBuffer)
	monitorMutex.Lock()
	monitorSubs[monitorChannel] = struct{}{}
	monitorMutex.Unlock()
	return monitorChannel
}

func MonitorUnsubscribe(monitorChannel chan resp.Value) {
	monitorMutex.Lock()
	delete(monitorSubs, monitorChannel)
	close(monitorChannel)
	monitorMutex.Unlock()
}

func MonitorPublish(cmd string, args []resp.Value) {
	monitorMutex.RLock()
	defer monitorMutex.RUnlock()

	if len(monitorSubs) == 0 {
		return
	}

	var builder strings.Builder
	builder.WriteString(cmd)
	for _, arg := range args {
		builder.WriteString(" ")
		builder.WriteString(arg.String)
	}

	message := stringReply(builder.String())
	for monitorChannel := range monitorSubs {
		select {
		case monitorChannel <- message:
		default:
		}
	}
}
