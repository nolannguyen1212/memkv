package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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
	waitOnce     sync.Once
	listenerMu   sync.Mutex
	connMutex    sync.Mutex
	connections  map[net.Conn]struct{}
	ready        chan struct{}
	stopped      chan struct{}
	shutdownErr  error
}

type Config struct {
	Address      string
	Auth         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	RESPLimits   resp.Limits
}

func New(config Config, ctx *engine.Context, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if config.RESPLimits == (resp.Limits{}) {
		config.RESPLimits = resp.DefaultLimits()
	}
	return &Server{
		config:       config,
		logger:       logger,
		ctx:          ctx,
		shutdownChan: make(chan struct{}),
		connections:  make(map[net.Conn]struct{}),
		ready:        make(chan struct{}),
		stopped:      make(chan struct{}),
	}
}

func (server *Server) ListenAndServe() error {
	listener, err := net.Listen(networkType, server.config.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.config.Address, err)
	}

	return server.Serve(listener)
}

func (server *Server) Serve(listener net.Listener) error {
	server.listenerMu.Lock()
	select {
	case <-server.shutdownChan:
		server.listenerMu.Unlock()
		return listener.Close()
	default:
		if server.listener != nil {
			server.listenerMu.Unlock()
			return errors.Join(fmt.Errorf("server already serving"), listener.Close())
		}
		server.listener = listener
		close(server.ready)
		server.listenerMu.Unlock()
	}
	server.logger.Info("server listening", "address", listener.Addr().String())

	return server.acceptConnections()
}

func (server *Server) Ready() <-chan struct{} {
	return server.ready
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
			if err := server.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				server.shutdownErr = err
			}
		}
		server.listenerMu.Unlock()
		server.shutdownErr = errors.Join(server.shutdownErr, server.closeConnections())
	})

	server.waitOnce.Do(func() {
		go func() {
			server.waitGroup.Wait()
			close(server.stopped)
		}()
	})

	select {
	case <-server.stopped:
		return server.shutdownErr
	case <-shutdownContext.Done():
		return errors.Join(server.shutdownErr, fmt.Errorf("close server: %w", shutdownContext.Err()))
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

func (server *Server) closeConnections() error {
	server.connMutex.Lock()
	defer server.connMutex.Unlock()
	var closeErr error
	for conn := range server.connections {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

func (server *Server) handleConnection(conn net.Conn) {
	connectionTransferred := server.serveConnection(conn)
	if !connectionTransferred {
		_ = conn.Close()
	}
}

func (server *Server) serveConnection(conn net.Conn) bool {
	connContext := engine.NewConnContext(server.ctx, conn)
	defer server.ctx.Disconnect(connContext.ID)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authenticated := server.config.Auth == ""

	for {
		if err := conn.SetReadDeadline(time.Now().Add(server.config.ReadTimeout)); err != nil {
			return false
		}

		request, err := resp.DecodeWithLimits(reader, server.config.RESPLimits)
		if err != nil {
			return false
		}

		command, valid := parseCommand(request)
		if !valid {
			if err := server.writeResponse(writer, conn, engine.Result{Type: engine.ResultError, String: "ERR invalid command frame"}); err != nil {
				return false
			}
			continue
		}
		cmdName := command.Name

		if !authenticated {
			handled, err := server.handleUnauthenticated(conn, writer, command, &authenticated, connContext)
			if err != nil {
				return false
			}
			if handled {
				continue
			}
		}
		if cmdName == "PSYNC" {
			response := connContext.SynchronizeReplica(command.Args)
			server.ctx.CommandProcessed()
			if response.Type == engine.ResultError {
				if err := server.writeResponse(writer, conn, response); err != nil {
					return false
				}
				continue
			}
			return true
		}

		response := engine.ExecuteCommand(connContext, command)
		server.ctx.CommandProcessed()

		if err := server.writeResponse(writer, conn, response); err != nil {
			return false
		}

	}
}

func parseCommand(request resp.Value) (engine.Command, bool) {
	if request.Type != resp.TypeArray || len(request.Array) == 0 {
		return engine.Command{}, false
	}
	for _, value := range request.Array {
		if value.IsNull || value.Type != resp.TypeBulkString && value.Type != resp.TypeSimpleString {
			return engine.Command{}, false
		}
	}
	args := make([]string, len(request.Array)-1)
	for index, value := range request.Array[1:] {
		args[index] = value.String
	}
	return engine.NewCommand(request.Array[0].String, args), true
}

func (server *Server) handleUnauthenticated(conn net.Conn, writer *bufio.Writer, command engine.Command, authenticated *bool, connContext *engine.ConnContext) (bool, error) {
	switch command.Name {
	case "AUTH":
		response := engine.ExecuteCommand(connContext, command)
		if response.Type == engine.ResultSimpleString && response.String == "OK" {
			*authenticated = true
		}
		return true, server.writeResponse(writer, conn, response)

	case "PING", "QUIT":
		return false, nil

	default:
		response := engine.Result{Type: engine.ResultError, String: "NOAUTH Authentication required."}
		return true, server.writeResponse(writer, conn, response)
	}
}

func (server *Server) writeResponse(writer *bufio.Writer, conn net.Conn, result engine.Result) error {
	if err := conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout)); err != nil {
		return fmt.Errorf("set response deadline for %s: %w", conn.RemoteAddr(), err)
	}
	if _, err := writer.WriteString(resp.Encode(resultToRESP(result))); err != nil {
		return fmt.Errorf("write response to %s: %w", conn.RemoteAddr(), err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush response to %s: %w", conn.RemoteAddr(), err)
	}
	return nil
}

func resultToRESP(result engine.Result) resp.Value {
	value := resp.Value{String: result.String, Number: result.Number, IsNull: result.IsNull}
	switch result.Type {
	case engine.ResultSimpleString:
		value.Type = resp.TypeSimpleString
	case engine.ResultError:
		value.Type = resp.TypeError
	case engine.ResultInteger:
		value.Type = resp.TypeInteger
	case engine.ResultBulkString:
		value.Type = resp.TypeBulkString
	case engine.ResultArray:
		value.Type = resp.TypeArray
		value.Array = make([]resp.Value, len(result.Array))
		for index, item := range result.Array {
			value.Array[index] = resultToRESP(item)
		}
	}
	return value
}
