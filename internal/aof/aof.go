package aof

import (
	"bufio"
	"os"
	"strconv"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
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

		if val.Type != resp.ARRAY || len(val.Array) == 0 {
			continue
		}

		cmdVal := val.Array[0]
		if cmdVal.Type != resp.STRING {
			continue
		}

		cmd := cmdVal.Str
		args := val.Array[1:]
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

func (a *AOF) Rewrite(dump func() map[string]store.Entry, path string) error {
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

	for key, e := range snapshot {
		v := resp.Value{
			Type: resp.ARRAY,
			Array: []resp.Value{
				{Type: resp.STRING, Str: "SET"},
				{Type: resp.BULKSTRING, Str: key},
				{Type: resp.BULKSTRING, Str: e.Value},
			},
		}

		if _, err := f.WriteString(resp.Encode(v)); err != nil {
			return err
		}

		if !e.ExpiredAt.IsZero() {
			at := e.ExpiredAt
			v := resp.Value{
				Type: resp.ARRAY,
				Array: []resp.Value{
					{Type: resp.STRING, Str: "PEXPIREAT"},
					{Type: resp.BULKSTRING, Str: key},
					{Type: resp.BULKSTRING, Str: strconv.FormatInt(at.UnixMilli(), 10)},
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
