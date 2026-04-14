package replication

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/core"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	core.Register("REPLCONF", handleReplconf)
	core.Register("PSYNC", handlePsync)
	core.Register("REPLICATION", handleReplication)
}

func handleReplconf(connContext *core.ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return protocol.Value{Type: protocol.TypeError, String: "ERR wrong number of arguments for 'replconf' command"}
	}
	subcommand := strings.ToUpper(args[0].String)
	switch subcommand {
	case "JOIN-APIKEY":
		if connContext.Replication == nil || !connContext.Replication.ValidateAPIKey(args[1].String) {
			return protocol.Value{Type: protocol.TypeError, String: "ERR invalid API key"}
		}
		connContext.ReplicaAPIKeyValidated = true
		return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
	case "LISTENING-ADDR":
		connContext.ReplicaAddress = args[1].String
		return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
	case "HEARTBEAT", "ACK":
		return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
	}
	return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
}

func handlePsync(connContext *core.ConnContext, args []protocol.Value) protocol.Value {
	if connContext.Replication == nil {
		return protocol.Value{Type: protocol.TypeError, String: "ERR replication not configured"}
	}
	if !connContext.ReplicaAPIKeyValidated {
		return protocol.Value{Type: protocol.TypeError, String: "ERR API key not validated"}
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
		return protocol.Value{Type: protocol.TypeError, String: "ERR " + err.Error()}
	}
	return protocol.Value{}
}

func handleReplication(connContext *core.ConnContext, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return protocol.Value{Type: protocol.TypeError, String: "ERR wrong number of arguments for 'replication' command"}
	}
	subcommand := strings.ToUpper(args[0].String)
	switch subcommand {
	case "APIKEY":
		if connContext.Replication == nil {
			return protocol.Value{Type: protocol.TypeError, String: "ERR replication not configured"}
		}
		return protocol.Value{Type: protocol.TypeBulkString, String: connContext.Replication.ServerAPIKey()}
	case "REPLICAS":
		if connContext.Replication == nil {
			return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{}}
		}
		addresses := connContext.Replication.ActiveReplicas()
		result := make([]protocol.Value, len(addresses))
		for i, addr := range addresses {
			result[i] = protocol.Value{Type: protocol.TypeBulkString, String: addr}
		}
		return protocol.Value{Type: protocol.TypeArray, Array: result}
	}
	return protocol.Value{Type: protocol.TypeError, String: "ERR unknown subcommand '" + args[0].String + "'"}
}
