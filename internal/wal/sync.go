package wal

import (
	"fmt"
	"os"
	"time"
)

type SyncPolicy string

const (
	// SyncAlways fsyncs before every acknowledged write.
	SyncAlways SyncPolicy = "always"
	// SyncEverySec fsyncs in the background once per second, so a crash can lose
	// up to one second of acknowledged writes.
	SyncEverySec SyncPolicy = "everysec"
	// SyncNo never fsyncs on the write path and leaves flushing to the OS,
	// except on Close.
	SyncNo SyncPolicy = "no"
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

// Sync flushes every record appended since the last sync. It is a no-op when
// nothing is pending.
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
