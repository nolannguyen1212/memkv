package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
	Null         Type = '_'
)

type Value struct {
	Type   Type
	Text   string
	Number int64
	Items  []Value
	IsNil  bool
}

func Encode(v Value) string {
	switch v.Type {
	case SimpleString:
		return fmt.Sprintf("+%s\r\n", v.Text)
	case Error:
		return fmt.Sprintf("-%s\r\n", v.Text)
	case Integer:
		return fmt.Sprintf(":%d\r\n", v.Number)
	case BulkString:
		if v.IsNil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v.Text), v.Text)
	case Array:
		if v.IsNil {
			return "*-1\r\n"
		}
		out := fmt.Sprintf("*%d\r\n", len(v.Items))
		for _, e := range v.Items {
			out += Encode(e)
		}
		return out
	}
	return ""
}

func Decode(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch prefix {
	case '+':
		return readLineValue(r, SimpleString)
	case '-':
		return readLineValue(r, Error)
	case ':':
		return readInteger(r)
	case '$':
		return readBulkString(r)
	case '*':
		return readArray(r)
	}
	return Value{}, fmt.Errorf("unknown RESP type: %q", prefix)
}

func readLineValue(r *bufio.Reader, t Type) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: t, Text: line}, nil
}

func readInteger(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: Integer, Number: n}, nil
}

func readBulkString(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return Value{Type: BulkString, IsNil: true}, nil
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Value{}, err
	}
	return Value{Type: BulkString, Text: string(buf[:n])}, nil
}

func readArray(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}
	if count == -1 {
		return Value{Type: Array, IsNil: true}, nil
	}

	values := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		v, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		values = append(values, v)
	}
	return Value{Type: Array, Items: values}, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
