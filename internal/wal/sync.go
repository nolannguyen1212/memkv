package wal

import (
	"fmt"
	"os"
	"time"
)

type SyncPolicy string

const (
	SyncAlways   SyncPolicy = "always"
	SyncEverySec SyncPolicy = "everysec"
	SyncNo       SyncPolicy = "no"
)

var everySecInterval = time.Second

func ParseSyncPolicy(value string) (SyncPolicy, error) {
	switch policy := SyncPolicy(value); policy {
	case SyncAlways, SyncEverySec, SyncNo:
		return policy, nil
	default:
		return "", fmt.Errorf("unknown fsync policy %q, want always, everysec, or no", value)
	}
}

func syncOSFile(file *os.File) error {
	return file.Sync()
}

func (log *Log) Sync() error {
	if !log.enabled {
		return nil
	}

	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.syncLocked()
}

func (log *Log) syncLocked() error {
	if !log.dirty {
		return nil
	}

	if err := log.syncFile(log.file); err != nil {
		return err
	}

	log.dirty = false
	return nil
}

func (log *Log) runBackgroundSync(stop <-chan struct{}, stopped chan<- struct{}) {
	defer close(stopped)
	ticker := time.NewTicker(everySecInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			log.mutex.Lock()

			if err := log.syncLocked(); err != nil && log.syncErr == nil {
				log.syncErr = err
			}

			log.mutex.Unlock()
		}
	}
}
