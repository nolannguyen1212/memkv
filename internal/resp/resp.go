package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ValueType byte

const (
	TypeSimpleString ValueType = '+'
	TypeError        ValueType = '-'
	TypeInteger      ValueType = ':'
	TypeBulkString   ValueType = '$'
	TypeArray        ValueType = '*'
)

const (
	NullBulkString = "$-1\r\n"
	NullArray      = "*-1\r\n"
)

type Value struct {
	Type   ValueType
	String string
	Number int64
	Array  []Value
	IsNull bool
}

func Encode(value Value) string {
	switch value.Type {
	case TypeSimpleString:
		return fmt.Sprintf("+%s\r\n", value.String)
	case TypeError:
		return fmt.Sprintf("-%s\r\n", value.String)
	case TypeInteger:
		return fmt.Sprintf(":%d\r\n", value.Number)
	case TypeBulkString:
		if value.IsNull {
			return NullBulkString
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value.String), value.String)
	case TypeArray:
		if value.IsNull {
			return NullArray
		}
		encoded := fmt.Sprintf("*%d\r\n", len(value.Array))
		for _, item := range value.Array {
			encoded += Encode(item)
		}
		return encoded
	}
	return ""
}

func Decode(reader *bufio.Reader) (Value, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+':
		return decodeSimpleString(reader)
	case '-':
		return decodeError(reader)
	case ':':
		return decodeInteger(reader)
	case '$':
		return decodeBulkString(reader)
	case '*':
		return decodeArray(reader)
	}

	return Value{}, fmt.Errorf("unknown RESP type: %q", prefix)
}

func decodeSimpleString(reader *bufio.Reader) (Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeSimpleString, String: line}, nil
}

func decodeError(reader *bufio.Reader) (Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeError, String: line}, nil
}

func decodeInteger(reader *bufio.Reader) (Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return Value{}, err
	}

	number, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}

	return Value{Type: TypeInteger, Number: number}, nil
}

func decodeBulkString(reader *bufio.Reader) (Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}

	if length == -1 {
		return Value{Type: TypeBulkString, IsNull: true}, nil
	}

	buffer := make([]byte, length+2)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return Value{}, err
	}

	return Value{Type: TypeBulkString, String: string(buffer[:length])}, nil
}

func decodeArray(reader *bufio.Reader) (Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return Value{}, err
	}

	count, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}

	if count == -1 {
		return Value{Type: TypeArray, IsNull: true}, nil
	}

	array := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		item, err := Decode(reader)
		if err != nil {
			return Value{}, err
		}
		array = append(array, item)
	}

	return Value{Type: TypeArray, Array: array}, nil
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
