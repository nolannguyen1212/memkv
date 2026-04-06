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
		return WrongArgCountError("subscribe")
	}

	channelName := args[0].String
	activeChannelName = channelName
	activeChannel = connContext.Store.PubSub.Subscribe(channelName)

	return ArrayResponse([]protocol.Value{
		StringResponse("subscribe"),
		StringResponse(channelName),
		IntegerResponse(1),
	})
}

func handleUnsubscribe(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if activeChannel == nil {
		return ErrorResponse("ERR not subscribed")
	}

	channelName := activeChannelName
	connContext.Store.PubSub.Unsubscribe(channelName, activeChannel)
	activeChannel = nil
	activeChannelName = ""

	return ArrayResponse([]protocol.Value{
		StringResponse("unsubscribe"),
		StringResponse(channelName),
		IntegerResponse(0),
	})
}

func handlePublish(connContext *ConnContext, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return WrongArgCountError("publish")
	}

	count := connContext.Store.PubSub.Publish(args[0].String, args[1].String)
	return IntegerResponse(int64(count))
}

func GetActiveChannel() datastructure.MessageChannel {
	return activeChannel
}
