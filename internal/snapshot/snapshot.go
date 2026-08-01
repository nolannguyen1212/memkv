package snapshot

import (
	"encoding/gob"
	"os"
	"sync"

	"github.com/william1nguyen/valkeydb/internal/store"
)

const (
	tempSuffix = ".tmp"
	filePerm   = 0644
)

type Data struct {
	KeyspaceData  map[string]store.KeyMetadata
	DictData      map[string]store.Entry
	SetData       map[string]store.SetEntry
	ListData      map[string][]string
	HashData      map[string]map[string]string
	SortedSetData map[string]map[string]float64
}

type File struct {
	mutex   sync.RWMutex
	file    *os.File
	enabled bool
}

func Open(path string, enabled bool) (*File, error) {
	if !enabled {
		return &File{enabled: false}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	return &File{file: f, enabled: true}, nil
}

func (r *File) Save(snapshot Data, path string) error {
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

func (r *File) Load(path string) (*Data, error) {
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

	var snapshot Data
	if err := gob.NewDecoder(f).Decode(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (r *File) Close() error {
	if !r.enabled {
		return nil
	}
	return r.file.Close()
}
