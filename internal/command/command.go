package command

import (
	"strings"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type Handler func(args []resp.Value) resp.Value

var registry = map[string]Handler{}

func Register(name string, h Handler) {
	registry[strings.ToUpper(name)] = h
}

func Lookup(name string) (Handler, bool) {
	h, ok := registry[strings.ToUpper(name)]
	return h, ok
}
