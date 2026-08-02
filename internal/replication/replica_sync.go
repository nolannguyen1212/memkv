package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/william1nguyen/valkeydb/internal/snapshot"
)

func (manager *Manager) synchronize(session *replicaSession, restore ApplySnapshot) error {
	replicationID, offset := manager.syncPosition()

	if err := session.send("PSYNC", replicationID, strconv.FormatInt(offset, 10)); err != nil {
		return fmt.Errorf("send psync: %w", err)
	}

	response, err := readLine(session.reader)

	if err != nil {
		return fmt.Errorf("read psync response: %w", err)
	}

	if strings.HasPrefix(response, "+CONTINUE") {
		return nil
	}

	if !strings.HasPrefix(response, "+FULLRESYNC") {
		return fmt.Errorf("unexpected psync response: %s", response)
	}

	newReplicationID, newOffset, err := parseFullResync(response)

	if err != nil {
		return err
	}

	if err := receiveSnapshot(session.reader, restore); err != nil {
		return fmt.Errorf("receive snapshot: %w", err)
	}

	manager.setSyncPosition(newReplicationID, newOffset)
	return nil
}

func (manager *Manager) syncPosition() (string, int64) {
	manager.mutex.RLock()
	replicationID := manager.primaryStreamID
	manager.mutex.RUnlock()

	if replicationID == "" {
		return "?", -1
	}

	return replicationID, manager.replicationOffset.Load()
}

func (manager *Manager) setSyncPosition(replicationID string, offset int64) {
	manager.mutex.Lock()
	manager.primaryStreamID = replicationID
	manager.mutex.Unlock()
	manager.replicationOffset.Store(offset)
}

func parseFullResync(response string) (string, int64, error) {
	parts := strings.Fields(response)

	if len(parts) != 3 {
		return "", 0, fmt.Errorf("invalid full resync response: %s", response)
	}

	offset, err := strconv.ParseInt(parts[2], 10, 64)

	if err != nil {
		return "", 0, fmt.Errorf("invalid full resync offset %q: %w", parts[2], err)
	}

	return parts[1], offset, nil
}

func receiveSnapshot(reader *bufio.Reader, restore ApplySnapshot) error {
	payload, err := readBulkPayload(reader)

	if err != nil {
		return err
	}

	data, err := snapshot.Decode(bytes.NewReader(payload))

	if err != nil {
		return err
	}

	return restore(data.State)
}

func readBulkPayload(reader *bufio.Reader) ([]byte, error) {
	header, err := readLine(reader)

	if err != nil {
		return nil, err
	}

	if len(header) < 2 {
		return nil, fmt.Errorf("expected bulk payload, got %s", header)
	}

	if header[0] != '$' {
		return nil, fmt.Errorf("expected bulk payload, got %s", header)
	}

	length, err := strconv.Atoi(header[1:])

	if err != nil {
		return nil, fmt.Errorf("invalid bulk length %q", header[1:])
	}

	if length < 0 {
		return nil, fmt.Errorf("invalid bulk length %q", header[1:])
	}

	if length > snapshot.MaxPayloadSize {
		return nil, fmt.Errorf("invalid bulk length %q", header[1:])
	}

	payload := make([]byte, length)

	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}

	trailer := make([]byte, 2)

	if _, err := io.ReadFull(reader, trailer); err != nil {
		return nil, err
	}

	if !bytes.Equal(trailer, []byte("\r\n")) {
		return nil, fmt.Errorf("invalid bulk trailer")
	}

	return payload, nil
}
