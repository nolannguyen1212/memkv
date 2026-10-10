package engine

import (
	"strings"

	"github.com/nolannguyen1212/memkv/internal/mutation"
)

type Command = mutation.Command

func NewCommand(name string, arguments []string) Command {
	return Command{Name: strings.ToUpper(name), Args: append([]string(nil), arguments...)}
}

func ExecuteCommand(connection *ConnContext, command Command) Result {
	result, err := connection.SubmitCommand(connection, command)

	if err != nil {
		return errorReply("ERR engine stopped")
	}

	return result
}
