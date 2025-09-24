package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func Decode(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+':
		return readString(r)
	case '-':
		return readError(r)
	case ':':
		return readInt(r)
	case '$':
		return readBulkString(r)
	case '*':
		return readArray(r)
	}

	return Value{}, fmt.Errorf("unknown RESP type: %q", prefix)
}

func readline(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(
		strings.TrimSuffix(line, "\n"),
		"\r",
	), nil
}

func readString(r *bufio.Reader) (Value, error) {
	line, _ := readline(r)
	return Value{
		Type: STRING,
		Str:  line,
	}, nil
}

func readError(r *bufio.Reader) (Value, error) {
	line, _ := readline(r)
	return Value{
		Type: ERROR,
		Str:  line,
	}, nil
}

func readInt(r *bufio.Reader) (Value, error) {
	line, _ := readline(r)
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}
	return Value{
		Type: INT,
		Int:  n,
	}, nil
}

func readBulkString(r *bufio.Reader) (Value, error) {
	line, _ := readline(r)
	length, _ := strconv.Atoi(line)
	if length == -1 {
		return Value{
			Type:  BULKSTRING,
			IsNil: true,
		}, nil
	}

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Value{}, err
	}
	return Value{
		Type: BULKSTRING,
		Str:  string(buf[:length]),
	}, nil
}

func readArray(r *bufio.Reader) (Value, error) {
	line, _ := readline(r)
	count, _ := strconv.Atoi(line)

	if count == -1 {
		return Value{
			Type:  ARRAY,
			IsNil: true,
		}, nil
	}

	items := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		val, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		items = append(items, val)
	}

	return Value{
		Type:  ARRAY,
		Array: items,
	}, nil
}
