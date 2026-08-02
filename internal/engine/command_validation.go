package engine

import (
	"strings"
)

type commandSpec struct {
	minArgs  int
	maxArgs  int
	validate func([]string) bool
}

var commandSpecs = map[string]commandSpec{
	"AUTH":        {1, 2, nil},
	"DEL":         {1, -1, nil},
	"DISCARD":     {0, 0, nil},
	"EXEC":        {0, 0, nil},
	"EXPIRE":      {2, 2, nil},
	"FLUSHALL":    {0, 0, nil},
	"FLUSHDB":     {0, 0, nil},
	"GET":         {1, 1, nil},
	"HDEL":        {2, -1, nil},
	"HEXISTS":     {2, 2, nil},
	"HGET":        {2, 2, nil},
	"HGETALL":     {1, 1, nil},
	"HLEN":        {1, 1, nil},
	"HSET":        {3, -1, oddAfterKey},
	"INFO":        {0, 1, nil},
	"KEYS":        {1, 1, nil},
	"LLEN":        {1, 1, nil},
	"LPOP":        {1, 2, nil},
	"LPUSH":       {2, -1, nil},
	"LRANGE":      {3, 3, nil},
	"MULTI":       {0, 0, nil},
	"PEXPIREAT":   {2, 2, nil},
	"PING":        {0, 1, nil},
	"PSYNC":       {0, 2, nil},
	"REPLCONF":    {2, 2, nil},
	"REPLICATION": {1, 1, nil},
	"RPOP":        {1, 2, nil},
	"RPUSH":       {2, -1, nil},
	"SADD":        {2, -1, nil},
	"SCARD":       {1, 1, nil},
	"SET":         {2, 4, validateSetSyntax},
	"SISMEMBER":   {2, 2, nil},
	"SMEMBERS":    {1, 1, nil},
	"SREM":        {2, -1, nil},
	"TTL":         {1, 1, nil},
	"UNWATCH":     {0, 0, nil},
	"WATCH":       {1, -1, nil},
	"ZADD":        {3, -1, validateZAddSyntax},
	"ZCARD":       {1, 1, nil},
	"ZRANGE":      {3, 3, nil},
	"ZRANK":       {2, 2, nil},
	"ZREM":        {2, -1, nil},
	"ZSCORE":      {2, 2, nil},
}

func validateCommand(name string, args []string) bool {
	spec, exists := commandSpecs[strings.ToUpper(name)]
	if !exists {
		return true
	}
	if len(args) < spec.minArgs || spec.maxArgs >= 0 && len(args) > spec.maxArgs {
		return false
	}
	return spec.validate == nil || spec.validate(args)
}

func oddAfterKey(args []string) bool {
	return len(args)%2 == 1
}

func validateSetSyntax(args []string) bool {
	if len(args) == 2 {
		return true
	}
	switch strings.ToUpper(args[2]) {
	case "EX", "PX", "EXAT", "PXAT":
		return true
	default:
		return false
	}
}

func validateZAddSyntax(args []string) bool {
	if len(args)%2 == 0 {
		return false
	}
	return true
}
