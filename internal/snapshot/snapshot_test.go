package snapshot

import (
	"bytes"
	"strings"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/store"
)

func TestCodecRoundTrip(t *testing.T) {
	data := Data{
		State: store.LogicalSnapshot{
			Keyspace: map[string]store.KeyMetadata{
				"key": {Type: store.KeyTypeString, Version: 7},
			},
			Strings: map[string]store.StringEntry{"key": {Value: "value"}},
		},
		CreatedAtUnixMilli: 1234,
		IncludedOffset:     42,
	}
	encoded, err := Encode(data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.IncludedOffset != 42 {
		t.Fatalf("IncludedOffset = %d, want 42", decoded.IncludedOffset)
	}
	if decoded.CreatedAtUnixMilli != 1234 {
		t.Fatalf("CreatedAtUnixMilli = %d, want 1234", decoded.CreatedAtUnixMilli)
	}
	if decoded.State.Strings["key"].Value != "value" {
		t.Fatalf("value = %q, want value", decoded.State.Strings["key"].Value)
	}
	if decoded.State.Keyspace["key"].Version != 7 {
		t.Fatalf("version = %d, want 7", decoded.State.Keyspace["key"].Version)
	}
}

func TestDecodeRejectsChecksumMismatch(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Decode() error = %v, want checksum error", err)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})
	if err != nil {
		t.Fatal(err)
	}
	encoded[5]++
	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("Decode() error = %v, want version error", err)
	}
}

func TestDecodeRejectsTruncatedPayload(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(bytes.NewReader(encoded[:len(encoded)-1])); err == nil {
		t.Fatal("Decode() accepted truncated payload")
	}
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 0)
	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("Decode() error = %v, want trailing data error", err)
	}
}
