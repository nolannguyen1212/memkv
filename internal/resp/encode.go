package resp

import (
	"fmt"
	"strconv"
)

func Encode(v Value) string {
	switch v.Type {
	case STRING:
		return fmt.Sprintf("+%s\r\n", v.Str)
	case ERROR:
		return fmt.Sprintf("-%s\r\n", v.Str)
	case INT:
		return fmt.Sprintf(":%s\r\n", strconv.FormatInt(v.Int, 10))
	case BULKSTRING:
		if v.IsNil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str)
	case ARRAY:
		if v.IsNil {
			return "*-1\r\n"
		}
		out := fmt.Sprintf("*%d\r\n", len(v.Array))
		for _, element := range v.Array {
			out += Encode(element)
		}
		return out
	}

	return ""
}
