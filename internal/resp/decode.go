package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

	"github.com/william1nguyen/valkeydb/internal/utils"
)

func Decode(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+':
		line, _ := utils.Readline(r)
		return Value{
			Type: STRING,
			Str:  line,
		}, nil

	case '-':
		line, _ := utils.Readline(r)
		return Value{
			Type: ERROR,
			Str:  line,
		}, nil

	case ':':
		line, _ := utils.Readline(r)
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{
			Type: INT,
			Int:  n,
		}, nil

	case '$':
		line, _ := utils.Readline(r)
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
	case '*':
		line, _ := utils.Readline(r)
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

	return Value{}, fmt.Errorf("unknown RESP type: %q", prefix)
}
