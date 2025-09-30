package command

import (
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

var (
	pubsub  *datastructure.Pubsub
	subChan chan resp.Value
	subName string
)

func SetPubsubContext(c *PubsubContext) {
	pubsub = c.Pubsub
}

type PubsubContext struct {
	Pubsub *datastructure.Pubsub
}

func InitPubsubCommands() {
	Register("SUBSCRIBE", cmdSubscribe)
	Register("UNSUBSCRIBE", cmdUnsubscribe)
	Register("PUBLISH", cmdPublish)
}

func cmdSubscribe(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'subscribe'"}
	}
	subName = args[0].Text
	subChan = pubsub.Subscribe(subName)
	return resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "subscribe"},
			{Type: resp.BulkString, Text: subName},
			{Type: resp.Integer, Number: 1},
		},
	}
}

func cmdUnsubscribe(args []resp.Value) resp.Value {
	if subChan == nil {
		return resp.Value{Type: resp.Error, Text: "ERR not subscribed"}
	}
	ch := subName
	pubsub.Unsubscribe(subName, subChan)
	subChan = nil
	subName = ""
	return resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "unsubscribe"},
			{Type: resp.BulkString, Text: ch},
			{Type: resp.Integer, Number: 0},
		},
	}
}

func cmdPublish(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Type: resp.Error, Text: "ERR wrong number of arguments for 'publish'"}
	}
	count := pubsub.Publish(args[0].Text, args[1].Text)
	return resp.Value{Type: resp.Integer, Number: int64(count)}
}

func GetSubChannel() <-chan resp.Value {
	return subChan
}
