package constant

var (
	RespNil             = []byte("$-1\r\n")
	RespOK              = []byte("+OK\r\n")
	RespZero            = []byte(":0\r\n")
	RespOne             = []byte(":1\r\n")
	RespEmptyArray      = []byte("*0\r\n")
	TtlKeyNotExist      = []byte(":2\r\n")
	TtlKeyExistNoExpire = []byte(":-1\r\n")
)
