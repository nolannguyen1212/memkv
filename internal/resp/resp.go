package resp

type Type byte

const (
	STRING     Type = '+'
	ERROR      Type = '-'
	INT        Type = ':'
	BULKSTRING Type = '$'
	ARRAY      Type = '*'
	NULL       Type = '_'
)

type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
	IsNil bool
}
