package command

import (
	"log"
	"path/filepath"

	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type SystemContext struct {
	DB *DB
}

var sysCtx *SystemContext

func SetSystemContext(c *SystemContext) { sysCtx = c }

func InitSystemCommands() {
	Register("BGSAVE", cmdBgsave)
	Register("KEYS", cmdKeys)
}

func cmdBgsave(args []resp.Value) resp.Value {
	go func() {
		filename := "dump.rdb"
		if len(args) > 0 && (args[0].Type == resp.BulkString || args[0].Type == resp.SimpleString) {
			filename = args[0].Text
		}

		snapshot := persistence.Snapshot{
			DictData: sysCtx.DB.Dict.Dump(),
			SetData:  sysCtx.DB.Set.Dump(),
		}

		if err := sysCtx.DB.RDB.Save(snapshot, filename); err != nil {
			log.Printf("BGSAVE error: %v", err)
		} else {
			log.Printf("BGSAVE success -> %s", filename)
		}
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

	items := make([]resp.Value, len(matchedKeys))
	for i, key := range matchedKeys {
		items[i] = resp.Value{Type: resp.BulkString, Text: key}
	}

	return resp.Value{Type: resp.Array, Items: items}
}
