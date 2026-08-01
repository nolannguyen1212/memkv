package engine

import (
	"net"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

func handleReplicaOf(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Type: resp.TypeError, String: "ERR wrong number of arguments for 'replicaof' command"}
	}

	if strings.ToUpper(args[0].String) == "NO" && len(args) >= 2 && strings.ToUpper(args[1].String) == "ONE" {
		if connContext.Replication != nil {
			connContext.Replication.Promote()
		}
		return resp.Value{Type: resp.TypeSimpleString, String: "OK"}
	}

	if len(args) < 2 {
		return resp.Value{Type: resp.TypeError, String: "ERR wrong number of arguments for 'replicaof' command"}
	}

	if connContext.Replication == nil {
		return resp.Value{Type: resp.TypeError, String: "ERR replication not configured"}
	}

	address := net.JoinHostPort(args[0].String, args[1].String)

	dispatchContext := NewConnContext(connContext.Context, nil)
	dispatch := func(cmd string, cmdArgs []resp.Value) {
		Execute(dispatchContext, cmd, cmdArgs)
	}

	connContext.Replication.SetReplica(address, "", "", dispatch)
	return resp.Value{Type: resp.TypeSimpleString, String: "OK"}
}
