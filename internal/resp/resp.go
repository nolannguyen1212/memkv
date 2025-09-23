package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Type byte

const (
	STRING     Type = '+'
	ERROR      Type = '-'
	INT        Type = ':'
	BULKSTRING Type = '$'
	ARRAY      Type = '*'
)

type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
}

func Read(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, nil
	}

	switch Type(prefix) {
	case BULKSTRING:
		line, err := r.ReadString('\n')
		if err != nil {
			return Value{}, err
		}

		length, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return Value{}, err
		}

		if length == -1 {
			return Value{
				Type: BULKSTRING,
				Str:  "",
			}, nil
		}

		buf := make([]byte, length+2) // +2 for \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return Value{}, err
		}

		return Value{
			Type: BULKSTRING,
			Str:  string(buf[:length]),
		}, nil

	case ARRAY:
		line, err := r.ReadString('\n')
		if err != nil {
			return Value{}, err
		}

		n, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return Value{}, err
		}

		values := make([]Value, n)
		for i := range n {
			element, err := Read(r)
			if err != nil {
				return Value{}, err
			}

			values[i] = element
		}

		return Value{
			Type:  ARRAY,
			Array: values,
		}, nil

	case STRING, ERROR:
		line, err := r.ReadString('\n')
		if err != nil {
			return Value{}, err
		}

		return Value{
			Type: Type(prefix),
			Str:  line[:len(line)-2],
		}, nil

	case INT:
		line, err := r.ReadString('\n')
		if err != nil {
			return Value{}, err
		}

		num, err := strconv.ParseInt(line[:len(line)-2], 10, 64)
		if err != nil {
			return Value{}, err
		}

		return Value{
			Type: INT,
			Int:  num,
		}, nil

	default:
		return Value{}, fmt.Errorf("unsupported RESP type: %c", prefix)
	}
}

func Write(w io.Writer, v Value) error {
	switch v.Type {
	case BULKSTRING:
		_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		return err
	case STRING:
		_, err := fmt.Fprintf(w, "+%s\r\n", v.Str)
		return err

	case ERROR:
		_, err := fmt.Fprintf(w, "-%s\r\n", v.Str)
		return err
	case INT:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Int)
		return err
	default:
		return fmt.Errorf("unsupported type for write: %v", v.Type)
	}
}
