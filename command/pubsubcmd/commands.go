package pubsubcmd

import (
	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/core/storage"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/protocol"
)

var (
	activeChannel     storage.MessageChannel
	activeChannelName string
)

func init() {
	command.Register("SUBSCRIBE", handleSubscribe)
	command.Register("UNSUBSCRIBE", handleUnsubscribe)
	command.Register("PUBLISH", handlePublish)
}

func handleSubscribe(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 1 {
		return command.WrongArgumentCountError("subscribe")
	}

	channelName := arguments[0].String
	activeChannelName = channelName
	activeChannel = dataStore.PubSub.Subscribe(channelName)

	return command.ArrayResponse([]protocol.Value{
		command.StringResponse("subscribe"),
		command.StringResponse(channelName),
		command.IntegerResponse(1),
	})
}

func handleUnsubscribe(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if activeChannel == nil {
		return command.ErrorResponse("ERR not subscribed")
	}

	channelName := activeChannelName
	dataStore.PubSub.Unsubscribe(channelName, activeChannel)
	activeChannel = nil
	activeChannelName = ""

	return command.ArrayResponse([]protocol.Value{
		command.StringResponse("unsubscribe"),
		command.StringResponse(channelName),
		command.IntegerResponse(0),
	})
}

func handlePublish(dataStore *store.Store, arguments []protocol.Value) protocol.Value {
	if len(arguments) != 2 {
		return command.WrongArgumentCountError("publish")
	}

	channelName := arguments[0].String
	message := arguments[1].String
	subscriberCount := dataStore.PubSub.Publish(channelName, message)

	return command.IntegerResponse(int64(subscriberCount))
}

func GetActiveChannel() storage.MessageChannel {
	return activeChannel
}
