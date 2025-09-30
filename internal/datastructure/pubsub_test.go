package datastructure

import (
	"testing"
	"time"
)

func TestPubsub(t *testing.T) {
	ps := CreatePubsub()

	ch := ps.Subscribe("news")

	count := ps.Publish("news", "hello")
	if count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}

	select {
	case msg := <-ch:
		if msg.Items[0].Text != "message" {
			t.Error("Expected message type")
		}
		if msg.Items[2].Text != "hello" {
			t.Error("Expected message 'hello'")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for message")
	}
}
