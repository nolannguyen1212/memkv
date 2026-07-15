package persistence

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

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
	isReplaying atomic.Bool
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
	if !aof.enabled || aof.isReplaying.Load() {
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

func (aof *AOF) Load(path string, dispatch func(cmd string, args []protocol.Value) error) error {
	if !aof.enabled {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	aof.isReplaying.Store(true)
	defer aof.isReplaying.Store(false)

	for {
		value, err := protocol.Decode(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode AOF: %w", err)
		}
		if value.Type != protocol.TypeArray || len(value.Array) == 0 {
			continue
		}
		cmdValue := value.Array[0]
		if cmdValue.Type != protocol.TypeBulkString && cmdValue.Type != protocol.TypeSimpleString {
			continue
		}
		if err := dispatch(cmdValue.String, value.Array[1:]); err != nil {
			return fmt.Errorf("replay %s: %w", cmdValue.String, err)
		}
	}
}

func (aof *AOF) IsReplaying() bool {
	return aof.isReplaying.Load()
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
	sortedSets map[string]map[string]float64,
	keyspace map[string]datastructure.KeyMetadata,
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
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	for key, entry := range dict {
		if err := writeCmd(file, "SET", key, entry.Value); err != nil {
			return err
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
		if err := writeCmd(file, append([]string{"SADD", key}, members...)...); err != nil {
			return err
		}
	}

	for key, values := range lists {
		if len(values) > 0 {
			if err := writeCmd(file, append([]string{"RPUSH", key}, values...)...); err != nil {
				return err
			}
		}
	}

	for key, hash := range hashes {
		for field, value := range hash {
			if err := writeCmd(file, "HSET", key, field, value); err != nil {
				return err
			}
		}
	}

	for key, members := range sortedSets {
		args := []string{"ZADD", key}
		for member, score := range members {
			args = append(args, strconv.FormatFloat(score, 'g', -1, 64), member)
		}
		if len(args) > 2 {
			if err := writeCmd(file, args...); err != nil {
				return err
			}
		}
	}

	for key, metadata := range keyspace {
		if !metadata.ExpiredAt.IsZero() {
			if err := writeCmd(file, "PEXPIREAT", key, strconv.FormatInt(metadata.ExpiredAt.UnixMilli(), 10)); err != nil {
				return err
			}
		}
	}

	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	newFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return err
	}
	oldFile := aof.file
	aof.file = newFile
	if oldFile != nil {
		_ = oldFile.Close()
	}
	return nil
}

func writeCmd(file *os.File, args ...string) error {
	arr := make([]protocol.Value, len(args))
	for i, arg := range args {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, String: arg}
	}
	_, err := file.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: arr}))
	return err
}
