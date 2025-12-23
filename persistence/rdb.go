package persistence

import (
	"encoding/gob"
	"io"
	"os"
	"sync"

	"github.com/william1nguyen/valkeydb/core/storage"
)

type Snapshot struct {
	DictData map[string]storage.Entry
	SetData  map[string]storage.SetEntry
	ListData map[string][]string
	HashData map[string]map[string]string
}

type RDB struct {
	mutex   sync.RWMutex
	file    *os.File
	enabled bool
}

func OpenRDB(path string, enabled bool) (*RDB, error) {
	if !enabled {
		return &RDB{enabled: false}, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, filePermissions)
	if err != nil {
		return nil, err
	}

	return &RDB{
		file:    file,
		enabled: true,
	}, nil
}

func (rdb *RDB) Save(snapshot Snapshot, path string) error {
	if !rdb.enabled {
		return nil
	}

	rdb.mutex.Lock()
	defer rdb.mutex.Unlock()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}

func (rdb *RDB) Load(path string) (*Snapshot, error) {
	if !rdb.enabled {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if fileInfo.Size() == 0 {
		return nil, nil
	}

	var snapshot Snapshot
	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(&snapshot); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		return nil, err
	}

	return &snapshot, nil
}

func (rdb *RDB) Close() error {
	if !rdb.enabled {
		return nil
	}
	return rdb.file.Close()
}
