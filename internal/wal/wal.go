package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

const (
	tempSuffix = ".tmp"
	filePerm   = 0644
)

type Log struct {
	mutex       sync.Mutex
	file        *os.File
	enabled     bool
	isReplaying atomic.Bool
}

func Open(path string, enabled bool) (*Log, error) {
	if !enabled {
		return &Log{enabled: false}, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	return &Log{file: file, enabled: true}, nil
}

func (log *Log) Append(value resp.Value) error {
	return log.append([]resp.Value{value})
}

func (log *Log) AppendBatch(values []resp.Value) error {
	return log.append(values)
}

func (log *Log) append(values []resp.Value) error {
	if !log.enabled || log.isReplaying.Load() {
		return nil
	}
	record, err := encodeRecord(values)
	if err != nil {
		return err
	}

	log.mutex.Lock()
	defer log.mutex.Unlock()

	if err := writeAllFile(log.file, record); err != nil {
		return err
	}
	return log.file.Sync()
}

func (log *Log) Load(path string, dispatch func(cmd string, args []resp.Value) error) error {
	return log.LoadBatches(path, func(commands []Command) error {
		for _, command := range commands {
			arguments := make([]resp.Value, len(command.Args))
			for index, argument := range command.Args {
				arguments[index] = resp.Value{Type: resp.TypeBulkString, String: argument}
			}
			if err := dispatch(command.Name, arguments); err != nil {
				return fmt.Errorf("replay %s: %w", command.Name, err)
			}
		}
		return nil
	})
}

func (log *Log) LoadBatches(path string, dispatch func([]Command) error) error {
	return log.LoadBatchesFrom(path, 0, dispatch)
}

func (log *Log) LoadBatchesFrom(path string, startOffset int64, dispatch func([]Command) error) error {
	if !log.enabled {
		return nil
	}
	if startOffset < 0 {
		return fmt.Errorf("WAL start offset cannot be negative")
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return err
	}

	log.isReplaying.Store(true)
	defer log.isReplaying.Store(false)

	offset := startOffset
	for {
		commands, recordSize, err := decodeRecord(file, offset)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := dispatch(commands); err != nil {
			return fmt.Errorf("replay WAL batch at offset %d: %w", offset, err)
		}
		offset += recordSize
	}
}

func (log *Log) Offset() (int64, error) {
	if !log.enabled {
		return 0, nil
	}
	log.mutex.Lock()
	defer log.mutex.Unlock()
	info, err := log.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (log *Log) RepairTail() error {
	if !log.enabled {
		return nil
	}
	log.mutex.Lock()
	defer log.mutex.Unlock()

	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var offset int64
	for {
		_, size, err := decodeRecord(log.file, offset)
		if err == io.EOF {
			_, seekErr := log.file.Seek(0, io.SeekEnd)
			return seekErr
		}
		if err == nil {
			offset += size
			continue
		}
		var truncated *truncatedRecordError
		if !errors.As(err, &truncated) {
			return err
		}
		if err := log.file.Truncate(offset); err != nil {
			return err
		}
		if err := log.file.Sync(); err != nil {
			return err
		}
		_, err = log.file.Seek(0, io.SeekEnd)
		return err
	}
}

func (log *Log) IsReplaying() bool {
	return log.isReplaying.Load()
}

func (log *Log) SizeMB() float64 {
	if !log.enabled || log.file == nil {
		return 0
	}
	info, err := log.file.Stat()
	if err != nil {
		return 0
	}
	return float64(info.Size()) / (1024 * 1024)
}

func (log *Log) Close() error {
	if !log.enabled {
		return nil
	}
	return log.file.Close()
}

func (log *Log) RewriteAll(
	state store.LogicalSnapshot,
	path string,
) error {
	if !log.enabled {
		return nil
	}

	log.mutex.Lock()
	defer log.mutex.Unlock()

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

	for key, entry := range state.Strings {
		if err := writeCmd(file, "SET", key, entry.Value); err != nil {
			return err
		}
	}

	for key, entry := range state.Sets {
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

	for key, values := range state.Lists {
		if len(values) > 0 {
			if err := writeCmd(file, append([]string{"RPUSH", key}, values...)...); err != nil {
				return err
			}
		}
	}

	for key, hash := range state.Hashes {
		for field, value := range hash {
			if err := writeCmd(file, "HSET", key, field, value); err != nil {
				return err
			}
		}
	}

	for key, members := range state.SortedSets {
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

	for key, metadata := range state.Keyspace {
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
	oldFile := log.file
	log.file = newFile
	if oldFile != nil {
		_ = oldFile.Close()
	}
	return nil
}

func writeCmd(file *os.File, args ...string) error {
	arr := make([]resp.Value, len(args))
	for i, arg := range args {
		arr[i] = resp.Value{Type: resp.TypeBulkString, String: arg}
	}
	record, err := encodeRecord([]resp.Value{{Type: resp.TypeArray, Array: arr}})
	if err != nil {
		return err
	}
	return writeAllFile(file, record)
}

func writeAllFile(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
