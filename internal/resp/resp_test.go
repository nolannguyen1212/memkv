package resp

import (
	"bufio"
	"strings"
	"testing"
)

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

func FuzzDecode(f *testing.F) {
	for _, seed := range []string{"+OK\r\n", "$3\r\nkey\r\n", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", "*-1\r\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = Decode(bufio.NewReader(strings.NewReader(input)))
	})
}
