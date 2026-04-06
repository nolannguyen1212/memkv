package datastructure

import "sync"

const pubsubBuffer = 100

type Message struct {
	Channel string
	Content string
}

type MessageChannel chan Message

type PubSub struct {
	mutex       sync.RWMutex
	subscribers map[string][]MessageChannel
}

func NewPubSub() *PubSub {
	return &PubSub{subscribers: make(map[string][]MessageChannel)}
}

func (pubSub *PubSub) Subscribe(channel string) MessageChannel {
	pubSub.mutex.Lock()
	defer pubSub.mutex.Unlock()

	messageChannel := make(MessageChannel, pubsubBuffer)
	pubSub.subscribers[channel] = append(pubSub.subscribers[channel], messageChannel)
	return messageChannel
}

func (pubSub *PubSub) Unsubscribe(channel string, messageChannel MessageChannel) {
	pubSub.mutex.Lock()
	defer pubSub.mutex.Unlock()

	subscribers := pubSub.subscribers[channel]
	for i, subscriber := range subscribers {
		if subscriber == messageChannel {
			pubSub.subscribers[channel] = append(subscribers[:i], subscribers[i+1:]...)
			close(messageChannel)
			if len(pubSub.subscribers[channel]) == 0 {
				delete(pubSub.subscribers, channel)
			}
			return
		}
	}
}

func (pubSub *PubSub) Publish(channel, content string) int {
	pubSub.mutex.RLock()
	defer pubSub.mutex.RUnlock()

	subscribers := pubSub.subscribers[channel]
	if len(subscribers) == 0 {
		return 0
	}

	message := Message{Channel: channel, Content: content}
	delivered := 0
	for _, messageChannel := range subscribers {
		select {
		case messageChannel <- message:
			delivered++
		default:
		}
	}
	return delivered
}
