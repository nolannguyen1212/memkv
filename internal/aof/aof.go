package aof

import (
	"bufio"
	"os"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/resp"
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
