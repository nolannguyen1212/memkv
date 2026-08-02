package wal

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/william1nguyen/memkv/internal/mutation"
)

type limitedWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (writer *limitedWriter) Write(data []byte) (int, error) {
	if len(data) > writer.limit {
		data = data[:writer.limit]
	}

	return writer.buffer.Write(data)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) {
	return 0, nil
}

func recordValue(name string, args ...string) mutation.Command {
	return mutation.New(name, args...)
}

func TestRecordRoundTrip(t *testing.T) {
	values := mutation.Batch{recordValue("SET", "key", "value"), recordValue("DEL", "other")}
	encoded, err := encodeRecord(values)

	if err != nil {
		t.Fatal(err)
	}

	decoded, size, err := decodeRecord(bytes.NewReader(encoded), 12)

	if err != nil {
		t.Fatal(err)
	}

	expected := []Command{{Name: "SET", Args: []string{"key", "value"}}, {Name: "DEL", Args: []string{"other"}}}

	if !reflect.DeepEqual(decoded, expected) {
		t.Fatalf("commands = %#v, want %#v", decoded, expected)
	}

	if size != int64(len(encoded)) {
		t.Fatalf("size = %d, want %d", size, len(encoded))
	}
}

func TestWriteAllHandlesPartialWrites(t *testing.T) {
	writer := &limitedWriter{limit: 3}
	data := []byte("partial writes")

	if err := writeAll(writer, data); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(writer.buffer.Bytes(), data) {
		t.Fatalf("written data = %q, want %q", writer.buffer.Bytes(), data)
	}

	if err := writeAll(zeroWriter{}, data); err != io.ErrShortWrite {
		t.Fatalf("writeAll() error = %v, want %v", err, io.ErrShortWrite)
	}
}

func FuzzDecodeRecord(f *testing.F) {
	encoded, err := encodeRecord(mutation.Batch{recordValue("SET", "key", "value")})

	if err != nil {
		f.Fatal(err)
	}

	f.Add(encoded)
	f.Add([]byte("VKWL"))
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _, _ = decodeRecord(bytes.NewReader(input), 0)
	})
}

func BenchmarkRecordCodec(b *testing.B) {
	value := recordValue("SET", "benchmark:key", "benchmark:value")

	for b.Loop() {
		encoded, err := encodeRecord(mutation.Batch{value})

		if err != nil {
			b.Fatal(err)
		}

		if _, _, err := decodeRecord(bytes.NewReader(encoded), 0); err != nil {
			b.Fatal(err)
		}
	}
}
