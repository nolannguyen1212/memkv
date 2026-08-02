package replication

import "sync"

type Backlog struct {
	buffer   []byte
	capacity int
	head     int64
	tail     int64
	mutex    sync.RWMutex
}

func NewBacklog(capacity int) *Backlog {
	return &Backlog{
		buffer:   make([]byte, capacity),
		capacity: capacity,
	}
}

func (backlog *Backlog) Write(data []byte) {
	backlog.mutex.Lock()
	defer backlog.mutex.Unlock()

	for _, b := range data {
		backlog.buffer[backlog.tail%int64(backlog.capacity)] = b
		backlog.tail++

		if backlog.tail-backlog.head > int64(backlog.capacity) {
			backlog.head = backlog.tail - int64(backlog.capacity)
		}
	}
}

func (backlog *Backlog) CurrentOffset() int64 {
	backlog.mutex.RLock()
	defer backlog.mutex.RUnlock()
	return backlog.tail
}

func (backlog *Backlog) CanServe(offset int64) bool {
	backlog.mutex.RLock()
	defer backlog.mutex.RUnlock()
	return backlog.head <= offset && offset <= backlog.tail
}

func (backlog *Backlog) ReadFrom(offset int64) []byte {
	backlog.mutex.RLock()
	defer backlog.mutex.RUnlock()

	if offset < backlog.head || offset > backlog.tail {
		return nil
	}

	result := make([]byte, int(backlog.tail-offset))

	for i := range result {
		result[i] = backlog.buffer[(offset+int64(i))%int64(backlog.capacity)]
	}

	return result
}
