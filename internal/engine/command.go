package engine

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/mutation"
)

type Command = mutation.Command

func NewCommand(name string, arguments []string) Command {
	return Command{Name: strings.ToUpper(name), Args: append([]string(nil), arguments...)}
}

func ExecuteCommand(connection *ConnContext, command Command) Result {
	return execute(connection, command)
}
