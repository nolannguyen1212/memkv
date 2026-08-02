package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/william1nguyen/memkv/internal/mutation"
	"github.com/william1nguyen/memkv/internal/store"
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

func (log *Log) Append(command mutation.Command) error {
	return log.append(mutation.Batch{command})
}

func (log *Log) AppendBatch(batch mutation.Batch) error {
	return log.append(batch)
}

func (log *Log) append(batch mutation.Batch) error {
	if !log.enabled || log.isReplaying.Load() {
		return nil
	}

	record, err := encodeRecord(batch)

	if err != nil {
		return err
	}

	log.mutex.Lock()
	defer log.mutex.Unlock()

	if err := writeAll(log.file, record); err != nil {
		return err
	}

	return log.file.Sync()
}

func (log *Log) Load(path string, dispatch func(mutation.Command) error) error {
	return log.LoadBatches(path, func(commands []Command) error {
		for _, command := range commands {
			if err := dispatch(command); err != nil {
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
	info, err := file.Stat()

	if err != nil {
		return err
	}

	if startOffset > info.Size() {
		return fmt.Errorf("WAL start offset %d exceeds file size %d", startOffset, info.Size())
	}

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
	defer removeTemporaryWAL(temp)

	if err := writeRewriteFile(temp, state); err != nil {
		return err
	}

	newFile, err := os.OpenFile(temp, os.O_APPEND|os.O_RDWR, filePerm)

	if err != nil {
		return err
	}

	if err := os.Rename(temp, path); err != nil {
		return errors.Join(err, newFile.Close())
	}

	oldFile := log.file
	log.file = newFile
	var closeErr error

	if oldFile != nil {
		closeErr = oldFile.Close()
	}

	return errors.Join(closeErr, syncDirectory(filepath.Dir(path)))
}

func writeRewriteFile(path string, state store.LogicalSnapshot) error {
	file, err := os.Create(path)

	if err != nil {
		return err
	}

	if err := writeLogicalSnapshot(file, state); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}

	return file.Close()
}

func writeLogicalSnapshot(file *os.File, state store.LogicalSnapshot) error {
	keys := state.FIFO

	if len(keys) == 0 {
		keys = sortedKeys(state.Keyspace)
	}

	for _, key := range keys {
		metadata := state.Keyspace[key]

		if err := writeSnapshotValue(file, state, key, metadata.Type); err != nil {
			return err
		}

		if !metadata.ExpiredAt.IsZero() {
			deadline := strconv.FormatInt(metadata.ExpiredAt.UnixMilli(), 10)

			if err := writeCmd(file, "PEXPIREAT", key, deadline); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeSnapshotValue(
	file *os.File,
	state store.LogicalSnapshot,
	key string,
	keyType store.KeyType,
) error {
	switch keyType {
	case store.KeyTypeString:
		return writeCmd(file, "SET", key, state.Strings[key].Value)
	case store.KeyTypeSet:
		args := append([]string{"SADD", key}, sortedKeys(state.Sets[key].Members)...)
		return writeCmd(file, args...)
	case store.KeyTypeList:
		args := append([]string{"RPUSH", key}, state.Lists[key]...)
		return writeCmd(file, args...)
	case store.KeyTypeHash:
		args := []string{"HSET", key}

		for _, field := range sortedKeys(state.Hashes[key]) {
			args = append(args, field, state.Hashes[key][field])
		}

		return writeCmd(file, args...)
	case store.KeyTypeSortedSet:
		args := []string{"ZADD", key}

		for _, member := range sortedKeys(state.SortedSets[key]) {
			score := state.SortedSets[key][member]
			args = append(args, strconv.FormatFloat(score, 'g', -1, 64), member)
		}

		return writeCmd(file, args...)
	default:
		return fmt.Errorf("cannot rewrite key %q with type %q", key, keyType)
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))

	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func removeTemporaryWAL(path string) {
	_ = os.Remove(path)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)

	if err != nil {
		return err
	}

	if err := directory.Sync(); err != nil {
		return errors.Join(err, directory.Close())
	}

	return directory.Close()
}

func writeCmd(file *os.File, args ...string) error {
	record, err := encodeRecord(mutation.Batch{mutation.New(args[0], args[1:]...)})

	if err != nil {
		return err
	}

	return writeAll(file, record)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)

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
