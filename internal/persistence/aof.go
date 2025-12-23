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
	cmdSet       = "SET"
	cmdSadd      = "SADD"
	cmdRpush     = "RPUSH"
	cmdHset      = "HSET"
	cmdPexpireat = "PEXPIREAT"
	tempSuffix   = ".tmp"
	filePerm     = 0644
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

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	return &AOF{file: f, enabled: true}, nil
}

func (a *AOF) Append(value protocol.Value) error {
	if !a.enabled || a.isReplaying {
		return nil
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	_, err := a.file.WriteString(protocol.Encode(value))
	if err != nil {
		return err
	}
	return a.file.Sync()
}

func (a *AOF) Load(path string, dispatch func(cmd string, args []protocol.Value)) error {
	if !a.enabled {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	a.isReplaying = true
	defer func() { a.isReplaying = false }()

	for {
		val, err := protocol.Decode(reader)
		if err != nil {
			break
		}
		if val.Type != protocol.TypeArray || len(val.Array) == 0 {
			continue
		}
		cmdVal := val.Array[0]
		if cmdVal.Type != protocol.TypeBulkString && cmdVal.Type != protocol.TypeSimpleString {
			continue
		}
		dispatch(cmdVal.String, val.Array[1:])
	}
	return nil
}

func (a *AOF) Close() error {
	if !a.enabled {
		return nil
	}
	return a.file.Close()
}

func (a *AOF) RewriteAll(
	dict map[string]datastructure.Entry,
	sets map[string]datastructure.SetEntry,
	lists map[string][]string,
	hashes map[string]map[string]string,
	path string,
) error {
	if !a.enabled {
		return nil
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	temp := path + tempSuffix
	f, err := os.Create(temp)
	if err != nil {
		return err
	}
	defer f.Close()

	for key, entry := range dict {
		writeCmd(f, cmdSet, key, entry.Value)
		if !entry.ExpiredAt.IsZero() {
			writeCmd(f, cmdPexpireat, key, strconv.FormatInt(entry.ExpiredAt.UnixMilli(), 10))
		}
	}

	for key, entry := range sets {
		if len(entry.Members) == 0 {
			continue
		}
		members := make([]string, 0, len(entry.Members))
		for m := range entry.Members {
			members = append(members, m)
		}
		writeCmd(f, append([]string{cmdSadd, key}, members...)...)
		if !entry.ExpiredAt.IsZero() {
			writeCmd(f, cmdPexpireat, key, strconv.FormatInt(entry.ExpiredAt.UnixMilli(), 10))
		}
	}

	for key, values := range lists {
		if len(values) > 0 {
			writeCmd(f, append([]string{cmdRpush, key}, values...)...)
		}
	}

	for key, hash := range hashes {
		for field, value := range hash {
			writeCmd(f, cmdHset, key, field, value)
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

func writeCmd(f *os.File, args ...string) {
	arr := make([]protocol.Value, len(args))
	for i, a := range args {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, String: a}
	}
	f.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: arr}))
}
