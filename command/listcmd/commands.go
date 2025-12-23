package listcmd

import (
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

func init() {
	command.Register("LPUSH", handleLeftPush)
	command.Register("RPUSH", handleRightPush)
	command.Register("LPOP", handleLeftPop)
	command.Register("RPOP", handleRightPop)
	command.Register("LLEN", handleLength)
	command.Register("LRANGE", handleRange)
	command.Register("SORT", handleSort)
}

func handleLeftPush(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("lpush")
	}

	key := arguments[0].String
	values := extractStrings(arguments[1:])
	listLength := dataStore.List.LeftPush(key, values...)

	return command.IntegerResponse(int64(listLength))
}

func handleRightPush(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 2 {
		return command.WrongArgumentCountError("rpush")
	}

	key := arguments[0].String
	values := extractStrings(arguments[1:])
	listLength := dataStore.List.RightPush(key, values...)

	return command.IntegerResponse(int64(listLength))
}

func handleLeftPop(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 || len(arguments) > 2 {
		return command.WrongArgumentCountError("lpop")
	}

	key := arguments[0].String
	count := 1

	if len(arguments) > 1 {
		parsedCount, err := strconv.Atoi(arguments[1].String)
		if err != nil {
			return command.NotIntegerError()
		}
		count = parsedCount
	}

	poppedValues := dataStore.List.LeftPop(key, count)
	return stringsToArrayResponse(poppedValues)
}

func handleRightPop(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 || len(arguments) > 2 {
		return command.WrongArgumentCountError("rpop")
	}

	key := arguments[0].String
	count := 1

	if len(arguments) > 1 {
		parsedCount, err := strconv.Atoi(arguments[1].String)
		if err != nil {
			return command.NotIntegerError()
		}
		count = parsedCount
	}

	poppedValues := dataStore.List.RightPop(key, count)
	return stringsToArrayResponse(poppedValues)
}

func handleLength(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("llen")
	}

	return command.IntegerResponse(int64(dataStore.List.Length(arguments[0].String)))
}

func handleRange(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 3 {
		return command.WrongArgumentCountError("lrange")
	}

	key := arguments[0].String

	start, err := strconv.Atoi(arguments[1].String)
	if err != nil {
		return command.NotIntegerError()
	}

	stop, err := strconv.Atoi(arguments[2].String)
	if err != nil {
		return command.NotIntegerError()
	}

	values, exists := dataStore.List.Range(key, start, stop)
	if !exists {
		return command.NullArrayResponse()
	}

	return stringsToArrayResponse(values)
}

func handleSort(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) < 1 {
		return command.WrongArgumentCountError("sort")
	}

	key := arguments[0].String
	ascending := true
	alphabetical := false

	for i := 1; i < len(arguments); i++ {
		option := strings.ToUpper(arguments[i].String)
		switch option {
		case "ASC":
			ascending = true
		case "DESC":
			ascending = false
		case "ALPHA":
			alphabetical = true
		default:
			return command.SyntaxError()
		}
	}

	dataStore.List.Sort(key, ascending, alphabetical)
	return command.OKResponse()
}

func extractStrings(values []protocol.Value) []string {
	strings := make([]string, len(values))
	for i, value := range values {
		strings[i] = value.String
	}
	return strings
}

func stringsToArrayResponse(values []string) protocol.Value {
	items := make([]protocol.Value, len(values))
	for i, value := range values {
		items[i] = command.StringResponse(value)
	}
	return command.ArrayResponse(items)
}
