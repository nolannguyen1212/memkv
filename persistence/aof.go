package persistence

import (
	"bufio"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/core/storage"
	"github.com/william1nguyen/valkeydb/protocol"
)

const (
	commandSet       = "SET"
	commandSadd      = "SADD"
	commandRpush     = "RPUSH"
	commandHset      = "HSET"
	commandPexpireat = "PEXPIREAT"
	tempFileSuffix   = ".tmp"
	filePermissions  = 0644
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

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePermissions)
	if err != nil {
		return nil, err
	}

	return &AOF{
		file:    file,
		enabled: true,
	}, nil
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

func (aof *AOF) Load(path string, dispatch func(commandName string, arguments []protocol.Value)) error {
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

		commandValue := value.Array[0]
		if commandValue.Type != protocol.TypeBulkString && commandValue.Type != protocol.TypeSimpleString {
			continue
		}

		dispatch(commandValue.String, value.Array[1:])
	}

	return nil
}

func (aof *AOF) Close() error {
	if !aof.enabled {
		return nil
	}
	return aof.file.Close()
}

func (aof *AOF) RewriteAll(
	dictSnapshot map[string]storage.Entry,
	setSnapshot map[string]storage.SetEntry,
	listSnapshot map[string][]string,
	hashSnapshot map[string]map[string]string,
	path string,
) error {
	if !aof.enabled {
		return nil
	}

	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	tempPath := path + tempFileSuffix
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	if err := aof.writeDictEntries(tempFile, dictSnapshot); err != nil {
		return err
	}

	if err := aof.writeSetEntries(tempFile, setSnapshot); err != nil {
		return err
	}

	if err := aof.writeListEntries(tempFile, listSnapshot); err != nil {
		return err
	}

	if err := aof.writeHashEntries(tempFile, hashSnapshot); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func (aof *AOF) writeDictEntries(file *os.File, snapshot map[string]storage.Entry) error {
	for key, entry := range snapshot {
		if err := aof.writeSetCommand(file, key, entry.Value); err != nil {
			return err
		}

		if !entry.ExpiredAt.IsZero() {
			if err := aof.writeExpireCommand(file, key, entry.ExpiredAt.UnixMilli()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (aof *AOF) writeSetEntries(file *os.File, snapshot map[string]storage.SetEntry) error {
	for key, entry := range snapshot {
		if len(entry.Members) == 0 {
			continue
		}

		if err := aof.writeSaddCommand(file, key, entry.Members); err != nil {
			return err
		}

		if !entry.ExpiredAt.IsZero() {
			if err := aof.writeExpireCommand(file, key, entry.ExpiredAt.UnixMilli()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (aof *AOF) writeListEntries(file *os.File, snapshot map[string][]string) error {
	for key, values := range snapshot {
		if len(values) == 0 {
			continue
		}

		items := make([]protocol.Value, 0, 2+len(values))
		items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: commandRpush})
		items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: key})

		for _, value := range values {
			items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: value})
		}

		_, err := file.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: items}))
		if err != nil {
			return err
		}
	}
	return nil
}

func (aof *AOF) writeHashEntries(file *os.File, snapshot map[string]map[string]string) error {
	for key, hash := range snapshot {
		for field, value := range hash {
			command := protocol.Value{
				Type: protocol.TypeArray,
				Array: []protocol.Value{
					{Type: protocol.TypeBulkString, String: commandHset},
					{Type: protocol.TypeBulkString, String: key},
					{Type: protocol.TypeBulkString, String: field},
					{Type: protocol.TypeBulkString, String: value},
				},
			}

			if _, err := file.WriteString(protocol.Encode(command)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (aof *AOF) writeSetCommand(file *os.File, key, value string) error {
	command := protocol.Value{
		Type: protocol.TypeArray,
		Array: []protocol.Value{
			{Type: protocol.TypeBulkString, String: commandSet},
			{Type: protocol.TypeBulkString, String: key},
			{Type: protocol.TypeBulkString, String: value},
		},
	}
	_, err := file.WriteString(protocol.Encode(command))
	return err
}

func (aof *AOF) writeSaddCommand(file *os.File, key string, members map[string]struct{}) error {
	items := make([]protocol.Value, 0, 2+len(members))
	items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: commandSadd})
	items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: key})

	for member := range members {
		items = append(items, protocol.Value{Type: protocol.TypeBulkString, String: member})
	}

	_, err := file.WriteString(protocol.Encode(protocol.Value{Type: protocol.TypeArray, Array: items}))
	return err
}

func (aof *AOF) writeExpireCommand(file *os.File, key string, expireAtMillis int64) error {
	command := protocol.Value{
		Type: protocol.TypeArray,
		Array: []protocol.Value{
			{Type: protocol.TypeBulkString, String: commandPexpireat},
			{Type: protocol.TypeBulkString, String: key},
			{Type: protocol.TypeBulkString, String: strconv.FormatInt(expireAtMillis, 10)},
		},
	}
	_, err := file.WriteString(protocol.Encode(command))
	return err
}

func BuildSetCommand(key, value string, timeToLive time.Duration) protocol.Value {
	items := []protocol.Value{
		{Type: protocol.TypeBulkString, String: commandSet},
		{Type: protocol.TypeBulkString, String: key},
		{Type: protocol.TypeBulkString, String: value},
	}
	return protocol.Value{Type: protocol.TypeArray, Array: items}
}
