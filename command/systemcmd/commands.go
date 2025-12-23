package systemcmd

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

const (
	defaultRDBFilename   = "dump.rdb"
	monitorChannelBuffer = 128
)

var (
	serverStartTime          time.Time
	backgroundSaveInProgress int32
	statisticsMutex          sync.Mutex
	totalCommandsProcessed   uint64
	totalConnectionsReceived uint64
	currentConnectionCount   int64
	monitorMutex             sync.RWMutex
	monitorSubscribers       = map[chan protocol.Value]struct{}{}
	configuredAuth           string
	aofEnabled               bool
	rdbEnabled               bool
)

func init() {
	command.Register("AUTH", handleAuth)
	command.Register("INFO", handleInfo)
	command.Register("BGSAVE", handleBackgroundSave)
	command.Register("KEYS", handleKeys)
	command.Register("MONITOR", handleMonitor)
	serverStartTime = time.Now()
}

func Configure(auth string, aof bool, rdb bool) {
	configuredAuth = auth
	aofEnabled = aof
	rdbEnabled = rdb
}

func handleAuth(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("auth")
	}

	if arguments[0].String != configuredAuth {
		return command.ErrorResponse("ERR invalid password")
	}

	return command.OKResponse()
}

func handleInfo(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	section := "all"
	if len(arguments) > 0 {
		section = strings.ToLower(arguments[0].String)
	}

	infoText := buildInfoString(dataStore, section)
	return command.StringResponse(infoText)
}

func buildInfoString(dataStore *store.Store, section string) string {
	var builder strings.Builder
	uptime := int(time.Since(serverStartTime).Seconds())
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	appendSection := func(name string, lines []string) {
		if section != "all" && section != name {
			return
		}
		for _, line := range lines {
			builder.WriteString(line)
			builder.WriteString("\n")
		}
	}

	appendSection("server", []string{
		"uptime_in_seconds:" + strconv.Itoa(uptime),
	})

	appendSection("clients", []string{
		"connected_clients:" + strconv.Itoa(int(GetCurrentConnections())),
		"total_connections_received:" + strconv.FormatUint(GetTotalConnections(), 10),
	})

	appendSection("memory", []string{
		"used_memory:" + strconv.FormatUint(memStats.Alloc, 10),
	})

	appendSection("persistence", []string{
		"aof_enabled:" + boolToString(aofEnabled),
		"rdb_enabled:" + boolToString(rdbEnabled),
		"bgsave_in_progress:" + strconv.Itoa(int(atomic.LoadInt32(&backgroundSaveInProgress))),
	})

	appendSection("stats", []string{
		"total_commands_processed:" + strconv.FormatUint(GetTotalCommands(), 10),
	})

	dictCount := len(dataStore.Dictionary.Snapshot())
	setCount := len(dataStore.Set.Snapshot())
	listCount := len(dataStore.List.Snapshot())
	hashCount := len(dataStore.HashMap.Snapshot())

	appendSection("keyspace", []string{
		fmt.Sprintf("db0:dict=%d,set=%d,list=%d,hash=%d", dictCount, setCount, listCount, hashCount),
	})

	return builder.String()
}

func handleBackgroundSave(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	go func() {
		atomic.StoreInt32(&backgroundSaveInProgress, 1)
		defer atomic.StoreInt32(&backgroundSaveInProgress, 0)
		log.Printf("BGSAVE completed")
	}()

	return protocol.Value{Type: protocol.TypeSimpleString, String: "Background saving started"}
}

func handleKeys(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 {
		return command.WrongArgumentCountError("keys")
	}

	pattern := arguments[0].String
	matchedKeys := collectMatchingKeys(dataStore, pattern)

	items := make([]protocol.Value, len(matchedKeys))
	for i, key := range matchedKeys {
		items[i] = command.StringResponse(key)
	}

	return command.ArrayResponse(items)
}

func collectMatchingKeys(dataStore *store.Store, pattern string) []string {
	var matchedKeys []string

	for key := range dataStore.Dictionary.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	for key := range dataStore.Set.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	for key := range dataStore.List.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	for key := range dataStore.HashMap.Snapshot() {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	return matchedKeys
}

func handleMonitor(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	return command.OKResponse()
}

func boolToString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func GetTotalCommands() uint64 {
	statisticsMutex.Lock()
	defer statisticsMutex.Unlock()
	return totalCommandsProcessed
}

func IncrementCommands() {
	statisticsMutex.Lock()
	totalCommandsProcessed++
	statisticsMutex.Unlock()
}

func GetTotalConnections() uint64 {
	statisticsMutex.Lock()
	defer statisticsMutex.Unlock()
	return totalConnectionsReceived
}

func GetCurrentConnections() int64 {
	statisticsMutex.Lock()
	defer statisticsMutex.Unlock()
	return currentConnectionCount
}

func IncrementConnections() {
	statisticsMutex.Lock()
	totalConnectionsReceived++
	currentConnectionCount++
	statisticsMutex.Unlock()
}

func DecrementConnections() {
	statisticsMutex.Lock()
	if currentConnectionCount > 0 {
		currentConnectionCount--
	}
	statisticsMutex.Unlock()
}

func MonitorSubscribe() chan protocol.Value {
	channel := make(chan protocol.Value, monitorChannelBuffer)
	monitorMutex.Lock()
	monitorSubscribers[channel] = struct{}{}
	monitorMutex.Unlock()
	return channel
}

func MonitorUnsubscribe(channel chan protocol.Value) {
	monitorMutex.Lock()
	delete(monitorSubscribers, channel)
	close(channel)
	monitorMutex.Unlock()
}

func MonitorPublish(commandName string, arguments []protocol.Value) {
	monitorMutex.RLock()
	defer monitorMutex.RUnlock()

	if len(monitorSubscribers) == 0 {
		return
	}

	message := buildMonitorMessage(commandName, arguments)
	for channel := range monitorSubscribers {
		select {
		case channel <- command.StringResponse(message):
		default:
		}
	}
}

func buildMonitorMessage(commandName string, arguments []protocol.Value) string {
	var builder strings.Builder
	builder.WriteString(commandName)
	for _, arg := range arguments {
		builder.WriteString(" ")
		builder.WriteString(arg.String)
	}
	return builder.String()
}
