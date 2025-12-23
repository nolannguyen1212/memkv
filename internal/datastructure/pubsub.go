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

func (ps *PubSub) Subscribe(channel string) MessageChannel {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ch := make(MessageChannel, pubsubBuffer)
	ps.subscribers[channel] = append(ps.subscribers[channel], ch)
	return ch
}

func (ps *PubSub) Unsubscribe(channel string, ch MessageChannel) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	subs := ps.subscribers[channel]
	for i, sub := range subs {
		if sub == ch {
			ps.subscribers[channel] = append(subs[:i], subs[i+1:]...)
			close(ch)
			if len(ps.subscribers[channel]) == 0 {
				delete(ps.subscribers, channel)
			}
			return
		}
	}
}

func (ps *PubSub) Publish(channel, content string) int {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	subs := ps.subscribers[channel]
	if len(subs) == 0 {
		return 0
	}

	msg := Message{Channel: channel, Content: content}
	delivered := 0
	for _, ch := range subs {
		select {
		case ch <- msg:
			delivered++
		default:
		}
	}
	return delivered
}
