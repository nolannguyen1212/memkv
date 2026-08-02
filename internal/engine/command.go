package engine

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type Command struct {
	Name string
	Args []string
}

func NewCommand(name string, arguments []resp.Value) Command {
	args := make([]string, len(arguments))
	for index, argument := range arguments {
		args[index] = argument.String
	}
	return Command{Name: strings.ToUpper(name), Args: args}
}

func ExecuteCommand(connection *ConnContext, command Command) resp.Value {
	return execute(connection, command)
}
