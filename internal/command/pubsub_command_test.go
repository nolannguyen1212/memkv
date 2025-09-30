package command

import (
	"testing"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

func TestPubsub(t *testing.T) {
	ps := datastructure.CreatePubsub()
	SetPubsubContext(&PubsubContext{Pubsub: ps})

	result := cmdSubscribe([]resp.Value{
		{Type: resp.BulkString, Text: "news"},
	})

	if result.Items[0].Text != "subscribe" {
		t.Error("Expected subscribe confirmation")
	}

	result = cmdPublish([]resp.Value{
		{Type: resp.BulkString, Text: "news"},
		{Type: resp.BulkString, Text: "hello"},
	})

	if result.Number != 1 {
		t.Errorf("Expected 1 subscriber, got %d", result.Number)
	}
}
