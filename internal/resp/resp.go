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

type Limits struct {
	MaxBulkLength  int
	MaxArrayLength int
	MaxDepth       int
	MaxLineLength  int
}

var defaultLimits = Limits{
	MaxBulkLength:  16 << 20,
	MaxArrayLength: 1024,
	MaxDepth:       8,
	MaxLineLength:  64 << 10,
}

func DefaultLimits() Limits {
	return defaultLimits
}

type Value struct {
	Type   ValueType
	String string
	Number int64
	Array  []Value
	IsNull bool
}

func Encode(value Value) string {
	var encoded strings.Builder
	encode(&encoded, value)
	return encoded.String()
}

func encode(encoded *strings.Builder, value Value) {
	switch value.Type {
	case TypeSimpleString:
		encoded.WriteByte('+')
		encoded.WriteString(value.String)
		encoded.WriteString("\r\n")
	case TypeError:
		encoded.WriteByte('-')
		encoded.WriteString(value.String)
		encoded.WriteString("\r\n")
	case TypeInteger:
		encoded.WriteByte(':')
		writeInt(encoded, value.Number)
		encoded.WriteString("\r\n")
	case TypeBulkString:
		if value.IsNull {
			encoded.WriteString(NullBulkString)
			return
		}
		encoded.WriteByte('$')
		writeInt(encoded, int64(len(value.String)))
		encoded.WriteString("\r\n")
		encoded.WriteString(value.String)
		encoded.WriteString("\r\n")
	case TypeArray:
		if value.IsNull {
			encoded.WriteString(NullArray)
			return
		}
		encoded.WriteByte('*')
		writeInt(encoded, int64(len(value.Array)))
		encoded.WriteString("\r\n")
		for _, item := range value.Array {
			encode(encoded, item)
		}
	}
}

func writeInt(encoded *strings.Builder, value int64) {
	buffer := strconv.AppendInt(nil, value, 10)
	encoded.Write(buffer)
}

func Decode(reader *bufio.Reader) (Value, error) {
	return DecodeWithLimits(reader, defaultLimits)
}

func DecodeWithLimits(reader *bufio.Reader, limits Limits) (Value, error) {
	if limits.MaxBulkLength <= 0 || limits.MaxArrayLength <= 0 || limits.MaxDepth <= 0 || limits.MaxLineLength <= 0 {
		return Value{}, fmt.Errorf("RESP limits must be positive")
	}
	return decode(reader, limits, 0)
}

func decode(reader *bufio.Reader, limits Limits, depth int) (Value, error) {
	if depth > limits.MaxDepth {
		return Value{}, fmt.Errorf("RESP nesting exceeds %d", limits.MaxDepth)
	}
	prefix, err := reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+':
		return decodeSimpleString(reader, limits)
	case '-':
		return decodeError(reader, limits)
	case ':':
		return decodeInteger(reader, limits)
	case '$':
		return decodeBulkString(reader, limits)
	case '*':
		return decodeArray(reader, limits, depth)
	}

	return Value{}, fmt.Errorf("unknown RESP type: %q", prefix)
}

func decodeSimpleString(reader *bufio.Reader, limits Limits) (Value, error) {
	line, err := readLine(reader, limits.MaxLineLength)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeSimpleString, String: line}, nil
}

func decodeError(reader *bufio.Reader, limits Limits) (Value, error) {
	line, err := readLine(reader, limits.MaxLineLength)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: TypeError, String: line}, nil
}

func decodeInteger(reader *bufio.Reader, limits Limits) (Value, error) {
	line, err := readLine(reader, limits.MaxLineLength)
	if err != nil {
		return Value{}, err
	}

	number, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}

	return Value{Type: TypeInteger, Number: number}, nil
}

func decodeBulkString(reader *bufio.Reader, limits Limits) (Value, error) {
	line, err := readLine(reader, limits.MaxLineLength)
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
	if length < -1 {
		return Value{}, fmt.Errorf("invalid bulk length %d", length)
	}
	if length > limits.MaxBulkLength {
		return Value{}, fmt.Errorf("bulk length %d exceeds %d", length, limits.MaxBulkLength)
	}

	buffer := make([]byte, length+2)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return Value{}, err
	}
	if buffer[length] != '\r' || buffer[length+1] != '\n' {
		return Value{}, fmt.Errorf("invalid bulk string trailer")
	}

	return Value{Type: TypeBulkString, String: string(buffer[:length])}, nil
}

func decodeArray(reader *bufio.Reader, limits Limits, depth int) (Value, error) {
	line, err := readLine(reader, limits.MaxLineLength)
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
	if count < -1 {
		return Value{}, fmt.Errorf("invalid array length %d", count)
	}
	if count > limits.MaxArrayLength {
		return Value{}, fmt.Errorf("array length %d exceeds %d", count, limits.MaxArrayLength)
	}

	array := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		item, err := decode(reader, limits, depth+1)
		if err != nil {
			return Value{}, err
		}
		array = append(array, item)
	}

	return Value{Type: TypeArray, Array: array}, nil
}

func readLine(reader *bufio.Reader, limit int) (string, error) {
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > limit+2 {
			return "", fmt.Errorf("RESP line exceeds %d", limit)
		}
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return "", err
		}
		break
	}
	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return "", fmt.Errorf("invalid RESP line ending")
	}
	return string(line[:len(line)-2]), nil
}
