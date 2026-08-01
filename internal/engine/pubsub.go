package engine

import (
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

var (
	activeChannel     store.MessageChannel
	activeChannelName string
)

func registerPubSubCommands(registry *Registry) {
	registry.Register("SUBSCRIBE", handleSubscribe)
	registry.Register("UNSUBSCRIBE", handleUnsubscribe)
	registry.Register("PUBLISH", handlePublish)
}

func handleSubscribe(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgCountError("subscribe")
	}

	channelName := args[0].String
	activeChannelName = channelName
	activeChannel = connContext.Store.PubSub.Subscribe(channelName)

	return arrayReply([]resp.Value{
		stringReply("subscribe"),
		stringReply(channelName),
		intReply(1),
	})
}

func handleUnsubscribe(connContext *ConnContext, args []resp.Value) resp.Value {
	if activeChannel == nil {
		return errorReply("ERR not subscribed")
	}

	channelName := activeChannelName
	connContext.Store.PubSub.Unsubscribe(channelName, activeChannel)
	activeChannel = nil
	activeChannelName = ""

	return arrayReply([]resp.Value{
		stringReply("unsubscribe"),
		stringReply(channelName),
		intReply(0),
	})
}

func handlePublish(connContext *ConnContext, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgCountError("publish")
	}

	count := connContext.Store.PubSub.Publish(args[0].String, args[1].String)
	return intReply(int64(count))
}

func GetActiveChannel() store.MessageChannel {
	return activeChannel
}
