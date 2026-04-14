package persistence

import (
	"bufio"
	"os"
	"strconv"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

const (
	tempSuffix = ".tmp"
	filePerm   = 0644
)

type AOF struct {
	mutex       sync.Mutex
	file        *os.File
	enabled     bool
	isReplaying bool
}

func OpenAOF(path string, enabled bool) (*AOF, error) {
	if !enabled {
		return &AOF{enabled: false}, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	return &AOF{file: file, enabled: true}, nil
}

func (aof *AOF) Append(value protocol.Value) error {
	if !aof.enabled || aof.isReplaying {
		return nil
	}

	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	_, err := aof.file.WriteString(protocol.Encode(value))
	if err != nil {
		return err
	}
	return aof.file.Sync()
}

func (aof *AOF) Load(path string, dispatch func(cmd string, args []protocol.Value)) error {
	if !aof.enabled {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	aof.isReplaying = true
	defer func() { aof.isReplaying = false }()

	for {
		value, err := protocol.Decode(reader)
		if err != nil {
			break
		}
		if value.Type != protocol.TypeArray || len(value.Array) == 0 {
			continue
		}
		cmdValue := value.Array[0]
		if cmdValue.Type != protocol.TypeBulkString && cmdValue.Type != protocol.TypeSimpleString {
			continue
		}
		dispatch(cmdValue.String, value.Array[1:])
	}
	return nil
}

func (aof *AOF) IsReplaying() bool {
	return aof.isReplaying
}

func (aof *AOF) SizeMB() float64 {
	if !aof.enabled || aof.file == nil {
		return 0
	}
	info, err := aof.file.Stat()
	if err != nil {
		return 0
	}
	return float64(info.Size()) / (1024 * 1024)
}

func (aof *AOF) Close() error {
	if !aof.enabled {
		return nil
	}
	return aof.file.Close()
}

func (aof *AOF) RewriteAll(
	dict map[string]datastructure.Entry,
	sets map[string]datastructure.SetEntry,
	lists map[string][]string,
	hashes map[string]map[string]string,
	path string,
) error {
	if !aof.enabled {
		return nil
	}

	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	temp := path + tempSuffix
	file, err := os.Create(temp)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, entry := range dict {
		writeCmd(file, "SET", key, entry.Value)
		if !entry.ExpiredAt.IsZero() {
			writeCmd(file, "PEXPIREAT", key, strconv.FormatInt(entry.ExpiredAt.UnixMilli(), 10))
		}
	}

	for key, entry := range sets {
		if len(entry.Members) == 0 {
			continue
		}
		members := make([]string, 0, len(entry.Members))
		for member := range entry.Members {
			members = append(members, member)
		}
		writeCmd(file, append([]string{"SADD", key}, members...)...)
		if !entry.ExpiredAt.IsZero() {
			writeCmd(file, "PEXPIREAT", key, strconv.FormatInt(entry.ExpiredAt.UnixMilli(), 10))
		}
	}

	for key, values := range lists {
		if len(values) > 0 {
			writeCmd(file, append([]string{"RPUSH", key}, values...)...)
		}
	}

	for key, hash := range hashes {
		for field, value := range hash {
			writeCmd(file, "HSET", key, field, value)
		}
	}

	if err := file.Sync(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

func writeCmd(file *os.File, args ...string) {
	arr := make([]protocol.Value, len(args))
	for i, arg := range args {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, String: arg}
	}
	file.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: arr}))
}
