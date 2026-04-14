package replication

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/core"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

func init() {
	core.Register("REPLICAOF", handleReplicaOf)
}

func handleReplicaOf(connContext *core.ConnContext, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return protocol.Value{Type: protocol.TypeError, String: "ERR wrong number of arguments for 'replicaof' command"}
	}

	if strings.ToUpper(args[0].String) == "NO" && len(args) >= 2 && strings.ToUpper(args[1].String) == "ONE" {
		if connContext.Replication != nil {
			connContext.Replication.Promote()
		}
		return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
	}

	if len(args) < 2 {
		return protocol.Value{Type: protocol.TypeError, String: "ERR wrong number of arguments for 'replicaof' command"}
	}

	if connContext.Replication == nil {
		return protocol.Value{Type: protocol.TypeError, String: "ERR replication not configured"}
	}

	address := args[0].String
	apiKey := args[1].String

	dispatchContext := core.NewConnContext(connContext.Context, nil)
	dispatch := func(cmd string, cmdArgs []protocol.Value) {
		core.Execute(dispatchContext, cmd, cmdArgs)
	}

	connContext.Replication.SetReplica(address, apiKey, dispatch)
	return protocol.Value{Type: protocol.TypeSimpleString, String: "OK"}
}
