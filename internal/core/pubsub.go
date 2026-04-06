package core

import (
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

var (
	activeChannel     datastructure.MessageChannel
	activeChannelName string
)

func init() {
	Register("SUBSCRIBE", handleSubscribe)
	Register("UNSUBSCRIBE", handleUnsubscribe)
	Register("PUBLISH", handlePublish)
}

func handleSubscribe(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgCountError("subscribe")
	}

	channelName := args[0].String
	activeChannelName = channelName
	activeChannel = connContext.Store.PubSub.Subscribe(channelName)

	return arrayReply([]protocol.Value{
		stringReply("subscribe"),
		stringReply(channelName),
		intReply(1),
	})
}

func handleUnsubscribe(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if activeChannel == nil {
		return errorReply("ERR not subscribed")
	}

	channelName := activeChannelName
	connContext.Store.PubSub.Unsubscribe(channelName, activeChannel)
	activeChannel = nil
	activeChannelName = ""

	return arrayReply([]protocol.Value{
		stringReply("unsubscribe"),
		stringReply(channelName),
		intReply(0),
	})
}

func handlePublish(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgCountError("publish")
	}

	count := connContext.Store.PubSub.Publish(args[0].String, args[1].String)
	return intReply(int64(count))
}

func GetActiveChannel() datastructure.MessageChannel {
	return activeChannel
}
