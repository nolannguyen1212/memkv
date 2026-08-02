package engine

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/memkv/internal/store"
)

func registerReplicationCommands(registry *Registry) {
	registry.register("REPLCONF", handleReplconf, arguments(2, 2), transactionControl)
	registry.register("PSYNC", handlePsync, arguments(2, 2), transactionControl)
	registry.register("REPLICATION", handleReplication, arguments(1, 1))
}

func handleReplconf(connContext *ConnContext, args []string) Result {
	if len(args) < 2 {
		return Result{Type: ResultError, String: "ERR wrong number of arguments for 'replconf' command"}
	}

	switch strings.ToUpper(args[0]) {
	case "LISTENING-ADDR":
		connContext.ReplicaAddress = args[1]
		return Result{Type: ResultSimpleString, String: "OK"}
	default:
		return Result{Type: ResultError, String: "ERR unsupported REPLCONF option"}
	}
}

func handlePsync(_ *ConnContext, _ []string) Result {
	return errorReply("ERR PSYNC must be handled by a server connection")
}

func (connContext *ConnContext) SynchronizeReplica(args []string) Result {
	if connContext.Replication == nil {
		return Result{Type: ResultError, String: "ERR replication not configured"}
	}

	if connContext.Replication.IsReplica() {
		return Result{Type: ResultError, String: "ERR PSYNC is only available on a primary"}
	}

	if len(args) != 2 {
		return wrongArgCountError("psync")
	}

	offset, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		return notIntegerError()
	}

	captureSnapshot := func() (store.LogicalSnapshot, int64, error) {
		return connContext.captureReplicationSnapshot()
	}

	err = connContext.Replication.HandlePSYNC(
		connContext.Connection,
		connContext.ReplicaAddress,
		args[0],
		offset,
		captureSnapshot,
	)

	if err != nil {
		return Result{Type: ResultError, String: "ERR " + err.Error()}
	}

	return Result{}
}

func handleReplication(connContext *ConnContext, args []string) Result {
	if len(args) == 0 {
		return Result{Type: ResultError, String: "ERR wrong number of arguments for 'replication' command"}
	}

	subcommand := strings.ToUpper(args[0])

	switch subcommand {
	case "REPLICAS":
		if connContext.Replication == nil {
			return Result{Type: ResultArray, Array: []Result{}}
		}

		addresses := connContext.Replication.ActiveReplicas()
		result := make([]Result, len(addresses))

		for i, addr := range addresses {
			result[i] = Result{Type: ResultBulkString, String: addr}
		}

		return Result{Type: ResultArray, Array: result}
	}

	return Result{Type: ResultError, String: "ERR unknown subcommand '" + args[0] + "'"}
}
