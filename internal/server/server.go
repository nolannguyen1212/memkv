package server

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/engine"
	"github.com/william1nguyen/valkeydb/internal/resp"
)

const networkType = "tcp"

type Server struct {
	config       Config
	logger       *slog.Logger
	listener     net.Listener
	ctx          *engine.Context
	shutdownChan chan struct{}
	waitGroup    sync.WaitGroup
	closeOnce    sync.Once
	listenerMu   sync.Mutex
	connMutex    sync.Mutex
	connections  map[net.Conn]struct{}
}

type Config struct {
	Address      string
	Auth         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func New(config Config, ctx *engine.Context, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		config:       config,
		logger:       logger,
		ctx:          ctx,
		shutdownChan: make(chan struct{}),
		connections:  make(map[net.Conn]struct{}),
	}
}

func (server *Server) ListenAndServe() error {
	listener, err := net.Listen(networkType, server.config.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.config.Address, err)
	}

	server.listenerMu.Lock()
	select {
	case <-server.shutdownChan:
		server.listenerMu.Unlock()
		_ = listener.Close()
		return nil
	default:
		server.listener = listener
		server.listenerMu.Unlock()
	}
	server.logger.Info("server listening", "address", listener.Addr().String())

	return server.acceptConnections()
}

func (server *Server) acceptConnections() error {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			select {
			case <-server.shutdownChan:
				return nil
			default:
			}
			server.logger.Error("accept connection", "error", err)
			continue
		}
		if !server.trackConnection(conn) {
			_ = conn.Close()
			continue
		}

		go func(conn net.Conn) {
			defer server.waitGroup.Done()
			defer server.untrackConnection(conn)
			server.ctx.ConnectionOpened()
			server.handleConnection(conn)
			server.ctx.ConnectionClosed()
		}(conn)
	}
}

func (server *Server) Close(shutdownContext context.Context) error {
	server.closeOnce.Do(func() {
		close(server.shutdownChan)
		server.listenerMu.Lock()
		if server.listener != nil {
			_ = server.listener.Close()
		}
		server.listenerMu.Unlock()
		server.closeConnections()
	})

	done := make(chan struct{})
	go func() {
		server.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-shutdownContext.Done():
		return fmt.Errorf("close server: %w", shutdownContext.Err())
	}
}

func (server *Server) trackConnection(conn net.Conn) bool {
	server.connMutex.Lock()
	defer server.connMutex.Unlock()
	select {
	case <-server.shutdownChan:
		return false
	default:
		server.connections[conn] = struct{}{}
		server.waitGroup.Add(1)
		return true
	}
}

func (server *Server) untrackConnection(conn net.Conn) {
	server.connMutex.Lock()
	delete(server.connections, conn)
	server.connMutex.Unlock()
}

func (server *Server) closeConnections() {
	server.connMutex.Lock()
	defer server.connMutex.Unlock()
	for conn := range server.connections {
		_ = conn.Close()
	}
}

func (server *Server) handleConnection(conn net.Conn) {
	connTaken := false
	defer func() {
		if !connTaken {
			_ = conn.Close()
		}
	}()

	connContext := engine.NewConnContext(server.ctx, conn)
	defer server.ctx.Disconnect(connContext.ID)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authenticated := server.config.Auth == ""

	for {
		if err := conn.SetReadDeadline(time.Now().Add(server.config.ReadTimeout)); err != nil {
			return
		}

		request, err := resp.Decode(reader)
		if err != nil {
			return
		}

		command, valid := server.parseCommand(request)
		if !valid {
			if err := server.writeResponse(writer, conn, resp.Value{Type: resp.TypeError, String: "ERR invalid command frame"}); err != nil {
				return
			}
			continue
		}
		cmdName := command.Name

		if !authenticated {
			if server.handleUnauthenticated(conn, writer, request, cmdName, &authenticated, connContext) {
				continue
			}
		}

		response := engine.ExecuteCommand(connContext, command)
		server.ctx.CommandProcessed()

		if cmdName == "PSYNC" && response.Type != resp.TypeError {
			connTaken = true
			return
		}

		if err := conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout)); err != nil {
			return
		}
		if err := server.writeResponse(writer, conn, response); err != nil {
			return
		}

	}
}

func (server *Server) parseCommand(request resp.Value) (engine.Command, bool) {
	if request.Type != resp.TypeArray || len(request.Array) == 0 {
		return engine.Command{}, false
	}
	for _, value := range request.Array {
		if value.Type != resp.TypeBulkString && value.Type != resp.TypeSimpleString {
			return engine.Command{}, false
		}
	}
	return engine.NewCommand(request.Array[0].String, request.Array[1:]), true
}

func (server *Server) handleUnauthenticated(conn net.Conn, writer *bufio.Writer, request resp.Value, cmdName string, authenticated *bool, connContext *engine.ConnContext) bool {
	switch cmdName {
	case "AUTH":
		response := engine.ExecuteCommand(connContext, engine.NewCommand(cmdName, request.Array[1:]))
		if response.Type == resp.TypeSimpleString && response.String == "OK" {
			*authenticated = true
		}
		_ = conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout))
		_ = server.writeResponse(writer, conn, response)
		return true

	case "PING", "QUIT":
		return false

	default:
		response := resp.Value{Type: resp.TypeError, String: "NOAUTH Authentication required."}
		_ = conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout))
		_ = server.writeResponse(writer, conn, response)
		return true
	}
}

func (server *Server) writeResponse(writer *bufio.Writer, conn net.Conn, value resp.Value) error {
	if _, err := writer.WriteString(resp.Encode(value)); err != nil {
		return fmt.Errorf("write response to %s: %w", conn.RemoteAddr(), err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush response to %s: %w", conn.RemoteAddr(), err)
	}
	return nil
}
