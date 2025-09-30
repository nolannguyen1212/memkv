package persistence

import (
	"bufio"
	"os"
	"strconv"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type AOF struct {
	mu        sync.Mutex
	file      *os.File
	enabled   bool
	replaying bool
}

func Open(path string, enabled bool) (*AOF, error) {
	if !enabled {
		return &AOF{
			enabled: enabled,
		}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &AOF{
		file:    f,
		enabled: true,
	}, nil
}

func (a *AOF) Append(v resp.Value) error {
	if !a.enabled || a.replaying {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.WriteString(resp.Encode(v))
	if err != nil {
		return err
	}

	return a.file.Sync()
}

func (a *AOF) Load(path string, dispatch func(cmd string, args []resp.Value)) error {
	if !a.enabled {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer f.Close()

	reader := bufio.NewReader(f)

	a.replaying = true
	defer func() {
		a.replaying = false
	}()

	for {
		val, err := resp.Decode(reader)
		if err != nil {
			break
		}

		if val.Type != resp.Array || len(val.Items) == 0 {
			continue
		}

		cmdVal := val.Items[0]
		if cmdVal.Type != resp.BulkString && cmdVal.Type != resp.SimpleString {
			continue
		}

		cmd := cmdVal.Text
		args := val.Items[1:]
		dispatch(cmd, args)
	}

	return nil
}

func (a *AOF) Close() error {
	if !a.enabled {
		return nil
	}

	return a.file.Close()
}

func (a *AOF) Rewrite(dump func() map[string]datastructure.Item, path string) error {
	if !a.enabled {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	defer f.Close()

	snapshot := dump()

	for key, item := range snapshot {
		if len(item.Members) > 0 {
			items := make([]resp.Value, 0, 2+len(item.Members))
			items = append(items, resp.Value{Type: resp.BulkString, Text: "SADD"})
			items = append(items, resp.Value{Type: resp.BulkString, Text: key})
			for member := range item.Members {
				items = append(items, resp.Value{Type: resp.BulkString, Text: member})
			}
			v := resp.Value{Type: resp.Array, Items: items}
			if _, err := f.WriteString(resp.Encode(v)); err != nil {
				return err
			}
		} else {
			v := resp.Value{
				Type: resp.Array,
				Items: []resp.Value{
					{Type: resp.BulkString, Text: "SET"},
					{Type: resp.BulkString, Text: key},
					{Type: resp.BulkString, Text: item.Value},
				},
			}
			if _, err := f.WriteString(resp.Encode(v)); err != nil {
				return err
			}
		}

		if !item.ExpiredAt.IsZero() {
			at := item.ExpiredAt
			v := resp.Value{
				Type: resp.Array,
				Items: []resp.Value{
					{Type: resp.BulkString, Text: "PEXPIREAT"},
					{Type: resp.BulkString, Text: key},
					{Type: resp.BulkString, Text: strconv.FormatInt(at.UnixMilli(), 10)},
				},
			}

			if _, err := f.WriteString(resp.Encode(v)); err != nil {
				return err
			}
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}
