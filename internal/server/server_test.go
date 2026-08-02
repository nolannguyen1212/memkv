package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/william1nguyen/memkv/internal/engine"
	"github.com/william1nguyen/memkv/internal/resp"
	"github.com/william1nguyen/memkv/internal/store"
)

type pipeListener struct {
	connection net.Conn
	closed     chan struct{}
	acceptOnce sync.Once
	closeOnce  sync.Once
}

func (listener *pipeListener) Accept() (net.Conn, error) {
	var connection net.Conn
	listener.acceptOnce.Do(func() {
		connection = listener.connection
	})

	if connection != nil {
		return connection, nil
	}

	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *pipeListener) Close() error {
	listener.closeOnce.Do(func() {
		close(listener.closed)
	})
	return nil
}

func (listener *pipeListener) Addr() net.Addr {
	return pipeAddress("pipe")
}

type pipeAddress string

func (address pipeAddress) Network() string { return string(address) }
func (address pipeAddress) String() string  { return string(address) }
func startTestServer(t testing.TB, auth string, listener net.Listener) *Server {
	t.Helper()
	execution := engine.NewContext(store.New(store.Config{}), nil, nil, engine.SystemConfig{Auth: auth})
	engineContext, stopEngine := context.WithCancel(context.Background())
	engineErrors := make(chan error, 1)
	go func() {
		engineErrors <- execution.Run(engineContext)
	}()
	<-execution.Ready()

	server := New(Config{
		Address:      listener.Addr().String(),
		Auth:         auth,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, execution, slog.New(slog.NewTextHandler(io.Discard, nil)))
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.Serve(listener)
	}()

	select {
	case <-server.Ready():
	case err := <-serverErrors:
		t.Fatalf("start server: %v", err)
	case <-time.After(time.Second):
		t.Fatal("server did not become ready")
	}

	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := server.Close(shutdownContext); err != nil {
			t.Errorf("close server: %v", err)
		}

		if err := <-serverErrors; err != nil {
			t.Errorf("serve: %v", err)
		}

		stopEngine()

		if err := <-engineErrors; err != nil {
			t.Errorf("run engine: %v", err)
		}
	})
	return server
}

func startServer(t testing.TB, auth string) net.Conn {
	t.Helper()
	serverConnection, clientConnection := net.Pipe()
	listener := &pipeListener{connection: serverConnection, closed: make(chan struct{})}
	startTestServer(t, auth, listener)
	t.Cleanup(func() {
		if err := clientConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close client connection: %v", err)
		}
	})
	return clientConnection
}

func writeAsync(connection net.Conn, request string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(connection, request)
		done <- err
	}()
	return done
}

func TestServerHandlesFragmentedAndPipelinedCommands(t *testing.T) {
	connection := startServer(t, "")
	reader := bufio.NewReader(connection)

	request := "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"

	for _, fragment := range []string{request[:1], request[1:7], request[7:18], request[18:]} {
		if _, err := io.WriteString(connection, fragment); err != nil {
			t.Fatal(err)
		}
	}

	response, err := resp.Decode(reader)

	if err != nil || response.String != "hello" {
		t.Fatalf("PING response = %#v, error = %v", response, err)
	}

	pipeline := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
	written := writeAsync(connection, pipeline)
	first, err := resp.Decode(reader)

	if err != nil || first.String != "OK" {
		t.Fatalf("SET response = %#v, error = %v", first, err)
	}

	second, err := resp.Decode(reader)

	if err != nil || second.String != "value" {
		t.Fatalf("GET response = %#v, error = %v", second, err)
	}

	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestInvalidRequestDoesNotStopServer(t *testing.T) {
	connection := startServer(t, "")
	reader := bufio.NewReader(connection)
	written := writeAsync(connection, ":1\r\n*1\r\n$4\r\nPING\r\n")
	invalid, err := resp.Decode(reader)

	if err != nil {
		t.Fatal(err)
	}

	if invalid.Type != resp.TypeError {
		t.Fatalf("response type = %v, want error", invalid.Type)
	}

	if !strings.Contains(invalid.String, "invalid command frame") {
		t.Fatalf("error = %q, want invalid command frame", invalid.String)
	}

	ping, err := resp.Decode(reader)

	if err != nil {
		t.Fatal(err)
	}

	if ping.String != "PONG" {
		t.Fatalf("PING response = %#v", ping)
	}

	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestServerRequiresAuthentication(t *testing.T) {
	connection := startServer(t, "secret")
	reader := bufio.NewReader(connection)
	request := "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*1\r\n$4\r\nPING\r\n"
	written := writeAsync(connection, request)
	responses := make([]resp.Value, 3)

	for index := range responses {
		value, err := resp.Decode(reader)

		if err != nil {
			t.Fatal(err)
		}

		responses[index] = value
	}

	if responses[0].Type != resp.TypeError {
		t.Fatalf("unauthenticated response = %#v, want error", responses[0])
	}

	if responses[1].String != "OK" {
		t.Fatalf("AUTH response = %#v, want OK", responses[1])
	}

	if responses[2].String != "PONG" {
		t.Fatalf("PING response = %#v, want PONG", responses[2])
	}

	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestServerOverTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Skipf("loopback listener unavailable: %v", err)
	}

	startTestServer(t, "", listener)
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close TCP connection: %v", err)
		}
	})

	if _, err := io.WriteString(connection, "*1\r\n$4\r\nPING\r\n"); err != nil {
		t.Fatal(err)
	}

	response, err := resp.Decode(bufio.NewReader(connection))

	if err != nil || response.String != "PONG" {
		t.Fatalf("PING response = %#v, error = %v", response, err)
	}
}

func TestServerRejectsSecondServe(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() { _ = clientConnection.Close() })
	listener := &pipeListener{connection: serverConnection, closed: make(chan struct{})}
	server := startTestServer(t, "", listener)
	second := &pipeListener{closed: make(chan struct{})}

	if err := server.Serve(second); err == nil || !strings.Contains(err.Error(), "already serving") {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestServerCloseIsIdempotentBeforeServe(t *testing.T) {
	server := New(Config{}, nil, nil)

	for range 2 {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		err := server.Close(shutdownContext)
		cancel()

		if err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzParseCommand(f *testing.F) {
	for _, seed := range []string{
		"*1\r\n$4\r\nPING\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		":1\r\n",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		value, err := resp.Decode(bufio.NewReader(strings.NewReader(input)))

		if err != nil {
			return
		}

		_, _ = parseCommand(value)
	})
}

func TestParseCommandRejectsNullBulkStrings(t *testing.T) {
	request := resp.Value{Type: resp.TypeArray, Array: []resp.Value{
		{Type: resp.TypeBulkString, String: "GET"},
		{Type: resp.TypeBulkString, IsNull: true},
	}}

	if _, valid := parseCommand(request); valid {
		t.Fatal("parseCommand() accepted a null bulk argument")
	}
}

func BenchmarkServerRoundTrip(b *testing.B) {
	connection := startServer(b, "")
	reader := bufio.NewReader(connection)
	setRequest := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	getRequest := "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"

	if _, err := io.WriteString(connection, setRequest); err != nil {
		b.Fatal(err)
	}

	if _, err := resp.Decode(reader); err != nil {
		b.Fatal(err)
	}

	benchmark := func(b *testing.B, request string) {
		for b.Loop() {
			if _, err := io.WriteString(connection, request); err != nil {
				b.Fatal(err)
			}

			if _, err := resp.Decode(reader); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("get", func(b *testing.B) {
		benchmark(b, getRequest)
	})
	b.Run("set", func(b *testing.B) {
		benchmark(b, setRequest)
	})
}
