package replication

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/william1nguyen/memkv/internal/resp"
)

type replicaSession struct {
	connection net.Conn
	reader     *bufio.Reader
}

func newReplicaSession(connection net.Conn) *replicaSession {
	return &replicaSession{
		connection: connection,
		reader:     bufio.NewReader(connection),
	}
}

func (session *replicaSession) send(args ...string) error {
	values := make([]resp.Value, len(args))

	for index, arg := range args {
		values[index] = resp.Value{Type: resp.TypeBulkString, String: arg}
	}

	request := resp.Value{Type: resp.TypeArray, Array: values}
	return writeAll(session.connection, []byte(resp.Encode(request)))
}

func (session *replicaSession) expect(expected string) error {
	response, err := readLine(session.reader)

	if err != nil {
		return err
	}

	if response != expected {
		return fmt.Errorf("expected %s, got %s", expected, response)
	}

	return nil
}

func (manager *Manager) handshake(session *replicaSession, username, password string) error {
	if password != "" {
		if err := authenticate(session, username, password); err != nil {
			return fmt.Errorf("authenticate: %w", err)
		}
	}

	if err := session.send("PING"); err != nil {
		return fmt.Errorf("send ping: %w", err)
	}

	if err := session.expect("+PONG"); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	if err := session.send("REPLCONF", "listening-addr", manager.listeningAddress); err != nil {
		return fmt.Errorf("send replication config: %w", err)
	}

	if err := session.expect("+OK"); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	return nil
}

func authenticate(session *replicaSession, username, password string) error {
	if username == "" {
		if err := session.send("AUTH", password); err != nil {
			return err
		}
	} else if err := session.send("AUTH", username, password); err != nil {
		return err
	}

	return session.expect("+OK")
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
