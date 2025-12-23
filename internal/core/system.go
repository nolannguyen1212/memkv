package core

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/william1nguyen/valkeydb/internal/protocol"
)

const monitorBuffer = 128

var (
	serverStartTime          time.Time
	backgroundSaveInProgress int32
	statsMutex               sync.Mutex
	totalCommands            uint64
	totalConnections         uint64
	currentConnections       int64
	monitorMutex             sync.RWMutex
	monitorSubs              = map[chan protocol.Value]struct{}{}
	configuredAuth           string
	aofEnabled               bool
	rdbEnabled               bool
)

func init() {
	Register("AUTH", handleAuth)
	Register("INFO", handleInfo)
	Register("BGSAVE", handleBgsave)
	Register("KEYS", handleKeys)
	Register("MONITOR", handleMonitor)
	serverStartTime = time.Now()
}

func ConfigureSystem(auth string, aof, rdb bool) {
	configuredAuth = auth
	aofEnabled = aof
	rdbEnabled = rdb
}

func handleAuth(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return WrongArgCountError("auth")
	}
	if args[0].String != configuredAuth {
		return ErrorResponse("ERR invalid password")
	}
	return OKResponse()
}

func handleInfo(ctx *Context, args []protocol.Value) protocol.Value {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0].String)
	}
	return StringResponse(buildInfo(ctx, section))
}

func buildInfo(ctx *Context, section string) string {
	var b strings.Builder
	uptime := int(time.Since(serverStartTime).Seconds())
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	add := func(name string, lines []string) {
		if section != "all" && section != name {
			return
		}
		for _, l := range lines {
			b.WriteString(l)
			b.WriteString("\n")
		}
	}

	add("server", []string{"uptime_in_seconds:" + strconv.Itoa(uptime)})
	add("clients", []string{
		"connected_clients:" + strconv.Itoa(int(GetCurrentConnections())),
		"total_connections_received:" + strconv.FormatUint(GetTotalConnections(), 10),
	})
	add("memory", []string{"used_memory:" + strconv.FormatUint(mem.Alloc, 10)})
	add("persistence", []string{
		"aof_enabled:" + boolStr(aofEnabled),
		"rdb_enabled:" + boolStr(rdbEnabled),
		"bgsave_in_progress:" + strconv.Itoa(int(atomic.LoadInt32(&backgroundSaveInProgress))),
	})
	add("stats", []string{"total_commands_processed:" + strconv.FormatUint(GetTotalCommands(), 10)})

	dictCount := len(ctx.Store.Dictionary.Snapshot())
	setCount := len(ctx.Store.Set.Snapshot())
	listCount := len(ctx.Store.List.Snapshot())
	hashCount := len(ctx.Store.HashMap.Snapshot())

	add("keyspace", []string{
		fmt.Sprintf("db0:dict=%d,set=%d,list=%d,hash=%d", dictCount, setCount, listCount, hashCount),
	})

	return b.String()
}

func handleBgsave(ctx *Context, args []protocol.Value) protocol.Value {
	go func() {
		atomic.StoreInt32(&backgroundSaveInProgress, 1)
		defer atomic.StoreInt32(&backgroundSaveInProgress, 0)
	}()
	return protocol.Value{Type: protocol.TypeSimpleString, String: "Background saving started"}
}

func handleKeys(ctx *Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return WrongArgCountError("keys")
	}

	pattern := args[0].String
	var keys []string

	for k := range ctx.Store.Dictionary.Snapshot() {
		if matched, _ := filepath.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}
	for k := range ctx.Store.Set.Snapshot() {
		if matched, _ := filepath.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}
	for k := range ctx.Store.List.Snapshot() {
		if matched, _ := filepath.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}
	for k := range ctx.Store.HashMap.Snapshot() {
		if matched, _ := filepath.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}

	return stringsToArray(keys)
}

func handleMonitor(ctx *Context, args []protocol.Value) protocol.Value {
	return OKResponse()
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

func MonitorSubscribe() chan protocol.Value {
	ch := make(chan protocol.Value, monitorBuffer)
	monitorMutex.Lock()
	monitorSubs[ch] = struct{}{}
	monitorMutex.Unlock()
	return ch
}

func MonitorUnsubscribe(ch chan protocol.Value) {
	monitorMutex.Lock()
	delete(monitorSubs, ch)
	close(ch)
	monitorMutex.Unlock()
}

func MonitorPublish(cmd string, args []protocol.Value) {
	monitorMutex.RLock()
	defer monitorMutex.RUnlock()

	if len(monitorSubs) == 0 {
		return
	}

	var b strings.Builder
	b.WriteString(cmd)
	for _, a := range args {
		b.WriteString(" ")
		b.WriteString(a.String)
	}

	msg := StringResponse(b.String())
	for ch := range monitorSubs {
		select {
		case ch <- msg:
		default:
		}
	}
}
