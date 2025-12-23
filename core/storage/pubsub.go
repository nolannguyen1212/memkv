package storage

import "sync"

const pubsubChannelBuffer = 100

type MessageChannel chan Message

type Message struct {
	Channel string
	Content string
}

type PubSub struct {
	mutex       sync.RWMutex
	subscribers map[string][]MessageChannel
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]MessageChannel),
	}
}

func (pubsub *PubSub) Subscribe(channelName string) MessageChannel {
	pubsub.mutex.Lock()
	defer pubsub.mutex.Unlock()

	subscriber := make(MessageChannel, pubsubChannelBuffer)
	pubsub.subscribers[channelName] = append(pubsub.subscribers[channelName], subscriber)

	return subscriber
}

func (pubsub *PubSub) Unsubscribe(channelName string, subscriber MessageChannel) {
	pubsub.mutex.Lock()
	defer pubsub.mutex.Unlock()

	subscribers := pubsub.subscribers[channelName]
	for i, existingSubscriber := range subscribers {
		if existingSubscriber == subscriber {
			pubsub.subscribers[channelName] = append(subscribers[:i], subscribers[i+1:]...)
			close(subscriber)

			if len(pubsub.subscribers[channelName]) == 0 {
				delete(pubsub.subscribers, channelName)
			}
			return
		}
	}
}

func (pubsub *PubSub) Publish(channelName, content string) int {
	pubsub.mutex.RLock()
	defer pubsub.mutex.RUnlock()

	subscribers := pubsub.subscribers[channelName]
	if len(subscribers) == 0 {
		return 0
	}

	message := Message{
		Channel: channelName,
		Content: content,
	}

	deliveredCount := 0
	for _, subscriber := range subscribers {
		select {
		case subscriber <- message:
			deliveredCount++
		default:
		}
	}

	return deliveredCount
}
