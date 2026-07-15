package persistence

import (
	"encoding/gob"
	"os"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/datastructure"
)

type Snapshot struct {
	KeyspaceData  map[string]datastructure.KeyMetadata
	DictData      map[string]datastructure.Entry
	SetData       map[string]datastructure.SetEntry
	ListData      map[string][]string
	HashData      map[string]map[string]string
	SortedSetData map[string]map[string]float64
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

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	return &RDB{file: f, enabled: true}, nil
}

func (r *RDB) Save(snapshot Snapshot, path string) error {
	if !r.enabled {
		return nil
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	temp := path + tempSuffix
	f, err := os.Create(temp)
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(snapshot); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(temp, path)
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
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}

	var snapshot Snapshot
	if err := gob.NewDecoder(f).Decode(&snapshot); err != nil {
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
