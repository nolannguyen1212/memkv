package command

import (
	"log"
	"path/filepath"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type SystemContext struct {
	DB *DB
}

var sysCtx *SystemContext

func SetSystemContext(c *SystemContext) { sysCtx = c }

func InitSystemCommands() {
	Register("AUTH", cmdAuth)
	Register("INFO", cmdInfo)
	Register("BGSAVE", cmdBgsave)
	Register("KEYS", cmdKeys)
	Register("MONITOR", cmdMonitor)
	startedAt = time.Now()
}

func cmdAuth(args []resp.Value) resp.Value {
	if len(args) > 1 {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR wrong number of arguments for 'auth'",
		}
	}

	auth := args[0].Text
	if auth != config.Global.GetAuth() {
		return resp.Value{
			Type: resp.Error,
			Text: "ERR auth is not correct",
		}
	}
	return resp.Value{
		Type: resp.SimpleString,
		Text: "OK",
	}
}

func cmdInfo(args []resp.Value) resp.Value {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0].Text)
	}
	var b strings.Builder
	uptime := int(time.Since(startedAt).Seconds())
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	dictCount := len(sysCtx.DB.Dict.Dump())
	setCount := len(sysCtx.DB.Set.Dump())
	listCount := len(sysCtx.DB.List.Dump())
	hashCount := len(sysCtx.DB.Hash.Dump())
	appendSection := func(name string, kv []string) {
		if section == "all" || section == name {
			for _, line := range kv {
				b.WriteString(line)
				b.WriteString("\n")
			}
		}
	}
	appendSection("server", []string{
		"uptime_in_seconds:" + strconv.Itoa(uptime),
	})
	appendSection("clients", []string{
		"connected_clients:" + strconv.Itoa(int(getCurrentConnections())),
		"total_connections_received:" + strconv.FormatUint(getTotalConnections(), 10),
	})
	appendSection("memory", []string{
		"used_memory:" + strconv.FormatUint(m.Alloc, 10),
	})
	appendSection("persistence", []string{
		"aof_enabled:" + boolToInt(config.Global.Persistence.AOF.Enabled),
		"rdb_enabled:" + boolToInt(config.Global.Persistence.RDB.Enabled),
		"bgsave_in_progress:" + strconv.Itoa(int(atomic.LoadInt32(&bgsaveInProg))),
	})
	appendSection("stats", []string{
		"total_commands_processed:" + strconv.FormatUint(getTotalCommands(), 10),
	})
	appendSection("keyspace", []string{
		fmt.Sprintf("db0:dict=%d,set=%d,list=%d,hash=%d", dictCount, setCount, listCount, hashCount),
	})
	return resp.Value{Type: resp.BulkString, Text: b.String()}
}

func cmdBgsave(args []resp.Value) resp.Value {
	go func() {
		filename := "dump.rdb"
		if len(args) > 0 && (args[0].Type == resp.BulkString || args[0].Type == resp.SimpleString) {
			filename = args[0].Text
		}

		atomic.StoreInt32(&bgsaveInProg, 1)
		snapshot := persistence.Snapshot{
			DictData: sysCtx.DB.Dict.Dump(),
			SetData:  sysCtx.DB.Set.Dump(),
			ListData: sysCtx.DB.List.Dump(),
			HashData: sysCtx.DB.Hash.Dump(),
		}

		if err := sysCtx.DB.RDB.Save(snapshot, filename); err != nil {
			log.Printf("BGSAVE error: %v", err)
		} else {
			log.Printf("BGSAVE success -> %s", filename)
		}
		atomic.StoreInt32(&bgsaveInProg, 0)
	}()
	return resp.Value{Type: resp.SimpleString, Text: "Background saving started"}
}

func cmdKeys(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'keys'"}
	}

	pattern := args[0].Text
	var matchedKeys []string

	dictSnapshot := sysCtx.DB.Dict.Dump()
	for key := range dictSnapshot {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	setSnapshot := sysCtx.DB.Set.Dump()
	for key := range setSnapshot {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	listSnapshot := sysCtx.DB.List.Dump()
	for key := range listSnapshot {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	hashSnapshot := sysCtx.DB.Hash.Dump()
	for key := range hashSnapshot {
		if matched, _ := filepath.Match(pattern, key); matched {
			matchedKeys = append(matchedKeys, key)
		}
	}

	items := make([]resp.Value, len(matchedKeys))
	for i, key := range matchedKeys {
		items[i] = resp.Value{Type: resp.BulkString, Text: key}
	}

	return resp.Value{Type: resp.Array, Items: items}
}

func cmdMonitor(args []resp.Value) resp.Value {
	return resp.Value{Type: resp.SimpleString, Text: "OK"}
}

var (
	startedAt     time.Time
	bgsaveInProg  int32
	statMu        sync.Mutex
	totalCmds     uint64
	totalConns    uint64
	currentConns  int64
	monMu         sync.RWMutex
	monSubs       = map[chan resp.Value]struct{}{}
)

func boolToInt(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func getTotalCommands() uint64 { statMu.Lock(); defer statMu.Unlock(); return totalCmds }
func incTotalCommands() { statMu.Lock(); totalCmds++; statMu.Unlock() }
func getTotalConnections() uint64 { statMu.Lock(); defer statMu.Unlock(); return totalConns }
func getCurrentConnections() int64 { statMu.Lock(); defer statMu.Unlock(); return currentConns }
func IncConnections() { statMu.Lock(); totalConns++; currentConns++; statMu.Unlock() }
func DecConnections() { statMu.Lock(); if currentConns > 0 { currentConns-- }; statMu.Unlock() }
func IncCommands() { incTotalCommands() }

func MonitorSubscribe() chan resp.Value { ch := make(chan resp.Value, 128); monMu.Lock(); monSubs[ch] = struct{}{}; monMu.Unlock(); return ch }
func MonitorUnsubscribe(ch chan resp.Value) { monMu.Lock(); delete(monSubs, ch); close(ch); monMu.Unlock() }
func MonitorPublish(cmd string, args []resp.Value) {
	monMu.RLock()
	if len(monSubs) == 0 {
		monMu.RUnlock()
		return
	}
	msg := buildMonitorLine(cmd, args)
	for ch := range monSubs {
		select { case ch <- resp.Value{Type: resp.BulkString, Text: msg}: default: }
	}
	monMu.RUnlock()
}

func buildMonitorLine(cmd string, args []resp.Value) string {
	var b strings.Builder
	b.WriteString(cmd)
	for _, a := range args {
		b.WriteString(" ")
		b.WriteString(a.Text)
	}
	return b.String()
}
