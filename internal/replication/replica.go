package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

type primaryConnectionWriter struct {
	connection net.Conn
	mutex      sync.Mutex
}

func (writer *primaryConnectionWriter) Send(args ...string) error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.Value{Type: resp.TypeBulkString, String: arg}
	}
	_, err := fmt.Fprint(writer.connection, resp.Encode(resp.Value{Type: resp.TypeArray, Array: values}))
	return err
}

func (manager *Manager) ConnectToPrimary(primaryAddress, username, password string, dispatch func(string, []resp.Value)) error {
	connection, err := net.Dial("tcp", primaryAddress)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	manager.mutex.Lock()
	manager.primaryConnection = connection
	manager.mutex.Unlock()
	defer func() {
		manager.mutex.Lock()
		if manager.primaryConnection == connection {
			manager.primaryConnection = nil
		}
		manager.mutex.Unlock()
	}()

	writer := &primaryConnectionWriter{connection: connection}
	reader := bufio.NewReader(connection)

	if password != "" {
		var err error
		if username == "" {
			err = writer.Send("AUTH", password)
		} else {
			err = writer.Send("AUTH", username, password)
		}
		if err != nil {
			return err
		}
		response, err := readLine(reader)
		if err != nil {
			return err
		}
		if response != "+OK" {
			return fmt.Errorf("authenticate to primary: %s", response)
		}
	}

	if err := writer.Send("PING"); err != nil {
		return err
	}
	pingResponse, err := readLine(reader)
	if err != nil {
		return err
	}
	if pingResponse != "+PONG" {
		return fmt.Errorf("ping primary: %s", pingResponse)
	}

	if err := writer.Send("REPLCONF", "listening-addr", manager.config.ListeningAddress); err != nil {
		return err
	}
	replconfResponse, err := readLine(reader)
	if err != nil {
		return err
	}
	if replconfResponse != "+OK" {
		return fmt.Errorf("configure replication: %s", replconfResponse)
	}

	manager.mutex.RLock()
	replicationID := manager.primaryStreamID
	manager.mutex.RUnlock()

	currentOffset := manager.replicationOffset.Load()
	psyncID, psyncOffset := replicationID, strconv.FormatInt(currentOffset, 10)
	if replicationID == "" {
		psyncID, psyncOffset = "?", "-1"
	}

	if err := writer.Send("PSYNC", psyncID, psyncOffset); err != nil {
		return err
	}

	syncResponse, err := readLine(reader)
	if err != nil {
		return err
	}

	switch {
	case strings.HasPrefix(syncResponse, "+FULLRESYNC"):
		log.Printf("replication: full sync from primary %s", primaryAddress)
		parts := strings.Fields(syncResponse)
		if len(parts) >= 2 {
			manager.mutex.Lock()
			manager.primaryStreamID = parts[1]
			manager.mutex.Unlock()
		}
		if len(parts) >= 3 {
			if offset, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
				manager.replicationOffset.Store(offset)
			}
		}
		if err := manager.receiveRDB(reader, dispatch); err != nil {
			return fmt.Errorf("receive RDB: %w", err)
		}
		log.Printf("replication: full sync complete")
	case strings.HasPrefix(syncResponse, "+CONTINUE"):
	default:
		return fmt.Errorf("unexpected PSYNC response: %s", syncResponse)
	}

	manager.streamLoop(writer, reader, dispatch)
	return nil
}

func (manager *Manager) receiveRDB(reader *bufio.Reader, dispatch func(string, []resp.Value)) error {
	line, err := readLine(reader)
	if err != nil {
		return err
	}
	if len(line) == 0 || line[0] != '$' {
		return fmt.Errorf("expected bulk RDB, got: %s", line)
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("invalid RDB length: %s", line)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return err
	}
	if trailer, err := reader.ReadString('\n'); err != nil {
		return err
	} else if trailer != "\r\n" {
		return fmt.Errorf("invalid RDB trailer")
	}
	dispatch("FLUSHALL", nil)

	dataReader := bufio.NewReader(bytes.NewReader(data))
	for {
		value, err := resp.Decode(dataReader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if value.Type == resp.TypeArray && len(value.Array) > 0 {
			dispatch(strings.ToUpper(value.Array[0].String), value.Array[1:])
		}
	}
	return nil
}

func (manager *Manager) streamLoop(writer *primaryConnectionWriter, reader *bufio.Reader, dispatch func(string, []resp.Value)) {
	for {
		value, err := resp.Decode(reader)
		if err != nil {
			return
		}
		if value.Type != resp.TypeArray || len(value.Array) == 0 {
			continue
		}

		encoded := []byte(resp.Encode(value))
		manager.backlog.Write(encoded)
		manager.replicationOffset.Add(int64(len(encoded)))

		cmd := strings.ToUpper(value.Array[0].String)
		log.Printf("[REPLICA] Received from primary: %s", resp.Encode(value))
		dispatch(cmd, value.Array[1:])

		offset := manager.replicationOffset.Load()
		if err := writer.Send("REPLCONF", "ACK", strconv.FormatInt(offset, 10)); err != nil {
			log.Printf("replication: failed to acknowledge offset %d: %v", offset, err)
			return
		}
	}
}

func (manager *Manager) reconnectLoop(stopChan <-chan struct{}, primaryAddress, username, password string, dispatch func(string, []resp.Value)) {
	const maxDelay = 30 * time.Second
	delay := time.Second

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		if err := manager.ConnectToPrimary(primaryAddress, username, password, dispatch); err != nil {
			log.Printf("replication: %s: %v; retry in %v", primaryAddress, err, delay)
		}

		select {
		case <-time.After(delay):
		case <-stopChan:
			return
		}

		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
