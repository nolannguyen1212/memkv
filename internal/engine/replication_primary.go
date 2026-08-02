package engine

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func registerReplicationCommands(registry *Registry) {
	registry.Register("REPLCONF", handleReplconf)
	registry.Register("PSYNC", handlePsync)
	registry.Register("REPLICATION", handleReplication)
}

func handleReplconf(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Type: resp.TypeError, String: "ERR wrong number of arguments for 'replconf' command"}
	}
	switch strings.ToUpper(args[0].String) {
	case "LISTENING-ADDR":
		connContext.ReplicaAddress = args[1].String
		return resp.Value{Type: resp.TypeSimpleString, String: "OK"}
	default:
		return resp.Value{Type: resp.TypeError, String: "ERR unsupported REPLCONF option"}
	}
}

func handlePsync(connContext *ConnContext, args []resp.Value) resp.Value {
	if connContext.Replication == nil {
		return resp.Value{Type: resp.TypeError, String: "ERR replication not configured"}
	}
	replicaID := "?"
	var offset int64 = -1
	if len(args) >= 2 {
		replicaID = args[0].String
		if off, err := strconv.ParseInt(args[1].String, 10, 64); err == nil {
			offset = off
		}
	}

	getRDB := func() []byte {
		return connContext.GenerateRDB()
	}

	if err := connContext.Replication.HandlePSYNC(connContext.Connection, connContext.ReplicaAddress, replicaID, offset, getRDB); err != nil {
		return resp.Value{Type: resp.TypeError, String: "ERR " + err.Error()}
	}
	return resp.Value{}
}

func handleReplication(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Type: resp.TypeError, String: "ERR wrong number of arguments for 'replication' command"}
	}
	subcommand := strings.ToUpper(args[0].String)
	switch subcommand {
	case "REPLICAS":
		if connContext.Replication == nil {
			return resp.Value{Type: resp.TypeArray, Array: []resp.Value{}}
		}
		addresses := connContext.Replication.ActiveReplicas()
		result := make([]resp.Value, len(addresses))
		for i, addr := range addresses {
			result[i] = resp.Value{Type: resp.TypeBulkString, String: addr}
		}
		return resp.Value{Type: resp.TypeArray, Array: result}
	}
	return resp.Value{Type: resp.TypeError, String: "ERR unknown subcommand '" + args[0].String + "'"}
}
