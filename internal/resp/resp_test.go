package resp

import (
	"bufio"
	"io"
	"reflect"
	"strings"
	"testing"
)

type fragmentedReader struct {
	reader io.Reader
}

func (reader fragmentedReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 1 {
		buffer = buffer[:1]
	}
	return reader.reader.Read(buffer)
}

func TestDecodeFragmentedFrames(t *testing.T) {
	values := []Value{
		{Type: TypeSimpleString, String: "OK"},
		{Type: TypeError, String: "ERR failure"},
		{Type: TypeInteger, Number: -42},
		{Type: TypeBulkString, String: "value"},
		{Type: TypeBulkString, IsNull: true},
		{Type: TypeArray, Array: []Value{{Type: TypeBulkString, String: "GET"}, {Type: TypeBulkString, String: "key"}}},
		{Type: TypeArray, IsNull: true},
	}
	for _, expected := range values {
		reader := bufio.NewReader(fragmentedReader{reader: strings.NewReader(Encode(expected))})
		actual, err := Decode(reader)
		if err != nil {
			t.Fatalf("Decode(%q): %v", Encode(expected), err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Decode(%q) = %#v, want %#v", Encode(expected), actual, expected)
		}
	}
}

func TestDecodeLimits(t *testing.T) {
	limits := Limits{MaxBulkLength: 4, MaxArrayLength: 2, MaxDepth: 2, MaxLineLength: 8}
	tests := []string{
		"$5\r\nvalue\r\n",
		"*3\r\n:1\r\n:2\r\n:3\r\n",
		"*1\r\n*1\r\n*1\r\n:1\r\n",
		"+123456789\r\n",
		"$1\r\naxx",
	}
	for _, input := range tests {
		if _, err := DecodeWithLimits(bufio.NewReader(strings.NewReader(input)), limits); err == nil {
			t.Fatalf("DecodeWithLimits(%q) succeeded", input)
		}
	}
}

func TestDecodePipelinedValues(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("+OK\r\n:2\r\n"))
	first, err := Decode(reader)
	if err != nil || first.String != "OK" {
		t.Fatalf("first value = %#v, error = %v", first, err)
	}
	second, err := Decode(reader)
	if err != nil || second.Number != 2 {
		t.Fatalf("second value = %#v, error = %v", second, err)
	}
}

func TestEncodeNestedArray(t *testing.T) {
	value := Value{Type: TypeArray, Array: []Value{
		{Type: TypeSimpleString, String: "OK"},
		{Type: TypeInteger, Number: -2},
		{Type: TypeBulkString, String: "value"},
		{Type: TypeBulkString, IsNull: true},
		{Type: TypeArray, Array: []Value{{Type: TypeInteger, Number: 1}}},
	}}
	expected := "*5\r\n+OK\r\n:-2\r\n$5\r\nvalue\r\n$-1\r\n*1\r\n:1\r\n"
	if encoded := Encode(value); encoded != expected {
		t.Fatalf("Encode() = %q, want %q", encoded, expected)
	}
}

func FuzzDecode(f *testing.F) {
	for _, seed := range []string{"+OK\r\n", "$3\r\nkey\r\n", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", "*-1\r\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = Decode(bufio.NewReader(strings.NewReader(input)))
	})
}

func BenchmarkCodec(b *testing.B) {
	value := Value{Type: TypeArray, Array: []Value{
		{Type: TypeBulkString, String: "SET"},
		{Type: TypeBulkString, String: "benchmark:key"},
		{Type: TypeBulkString, String: "benchmark:value"},
	}}
	encoded := Encode(value)
	b.Run("encode", func(b *testing.B) {
		for b.Loop() {
			_ = Encode(value)
		}
	})
	b.Run("decode", func(b *testing.B) {
		for b.Loop() {
			if _, err := Decode(bufio.NewReader(strings.NewReader(encoded))); err != nil {
				b.Fatal(err)
			}
		}
	})
}
