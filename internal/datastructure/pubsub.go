package datastructure

import (
	"sync"

	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type Pubsub struct {
	mu       sync.RWMutex
	channels map[string][]chan resp.Value
}

func CreatePubsub() *Pubsub {
	return &Pubsub{
		channels: make(map[string][]chan resp.Value),
	}
}

func (p *Pubsub) Subscribe(channel string) chan resp.Value {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := make(chan resp.Value, 100)
	p.channels[channel] = append(p.channels[channel], ch)
	return ch
}

func (p *Pubsub) Unsubscribe(channel string, ch chan resp.Value) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, sub := range p.channels[channel] {
		if sub == ch {
			p.channels[channel] = append(p.channels[channel][:i], p.channels[channel][i+1:]...)
			close(ch)
			if len(p.channels[channel]) == 0 {
				delete(p.channels, channel)
			}
			return
		}
	}
}

func (p *Pubsub) Publish(channel, message string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.channels[channel]) == 0 {
		return 0
	}

	msg := resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "message"},
			{Type: resp.BulkString, Text: channel},
			{Type: resp.BulkString, Text: message},
		},
	}

	count := 0
	for _, ch := range p.channels[channel] {
		select {
		case ch <- msg:
			count++
		default:
		}
	}
	return count
}
