package resp

import (
	"fmt"
	"strconv"
)

func Encode(v Value) string {
	switch v.Type {
	case STRING:
		return encodeString(v)
	case ERROR:
		return encodeError(v)
	case INT:
		return encodeInt(v)
	case BULKSTRING:
		return encodeBulkString(v)
	case ARRAY:
		return encodeArray(v)
	}

	return ""
}

func encodeString(v Value) string {
	return fmt.Sprintf("+%s\r\n", v.Str)
}

func encodeError(v Value) string {
	return fmt.Sprintf("-%s\r\n", v.Str)
}

func encodeInt(v Value) string {
	return fmt.Sprintf(":%s\r\n", strconv.FormatInt(v.Int, 10))
}

func encodeBulkString(v Value) string {
	if v.IsNil {
		return "$-1\r\n"
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str)
}

func encodeArray(v Value) string {
	if v.IsNil {
		return "*-1\r\n"
	}
	out := fmt.Sprintf("*%d\r\n", len(v.Array))
	for _, element := range v.Array {
		out += Encode(element)
	}
	return out
}
