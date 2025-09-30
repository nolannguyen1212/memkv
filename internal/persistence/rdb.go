package persistence

import (
	"encoding/gob"
	"io"
	"os"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
)

type Snapshot struct {
	DictData map[string]datastructure.Item
	SetData  map[string]datastructure.Item
}

type RDB struct {
	mu      sync.RWMutex
	file    *os.File
	enabled bool
}

func OpenRDB(path string, enabled bool) (*RDB, error) {
	if !enabled {
		return &RDB{
			enabled: false,
		}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &RDB{
		file:    f,
		enabled: true,
	}, nil
}

func (r *RDB) Save(snapshot Snapshot, path string) error {
	if !r.enabled {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	encoder := gob.NewEncoder(f)
	return encoder.Encode(snapshot)
}

func (r *RDB) Load(path string) (*Snapshot, error) {
	if !r.enabled {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		return nil, nil
	}

	var snapshot Snapshot
	decoder := gob.NewDecoder(f)
	if err := decoder.Decode(&snapshot); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		return nil, err
	}

	return &snapshot, nil
}

func (r *RDB) Close() error {
	if !r.enabled {
		return nil
	}
	return r.file.Close()
}
