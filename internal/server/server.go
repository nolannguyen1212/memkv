package server

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/core"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol"
)

const networkType = "tcp"

type Server struct {
	address      string
	listener     net.Listener
	ctx          *core.Context
	aof          *persistence.AOF
	rdb          *persistence.RDB
	shutdownChan chan struct{}
	waitGroup    sync.WaitGroup
}

func New(address string, ctx *core.Context, aof *persistence.AOF, rdb *persistence.RDB) *Server {
	return &Server{
		address: address,
		ctx:     ctx,
		aof:     aof,
		rdb:     rdb,
	}
}

func (server *Server) ListenAndServe() error {
	listener, err := net.Listen(networkType, server.address)
	if err != nil {
		return err
	}

	server.listener = listener
	server.shutdownChan = make(chan struct{})

	server.startBackgroundTasks()
	log.Printf("listening on %s", server.address)

	return server.acceptConnections()
}

func (server *Server) startBackgroundTasks() {
	server.waitGroup.Add(1)
	go func() {
		defer server.waitGroup.Done()
		ticker := time.NewTicker(config.Global.AOFRewriteInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				server.rewriteAOF()
			case <-server.shutdownChan:
				return
			}
		}
	}()
}

func (server *Server) rewriteAOF() {
	aofFilename := config.Global.Persistence.AOF.Filename
	err := server.aof.RewriteAll(
		server.ctx.Store.Dictionary.Snapshot(),
		server.ctx.Store.Set.Snapshot(),
		server.ctx.Store.List.Snapshot(),
		server.ctx.Store.Hash.Snapshot(),
		aofFilename,
	)
	if err != nil {
		log.Printf("aof rewrite error: %v", err)
		return
	}
	log.Printf("aof rewrite done")
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
			log.Printf("accept error: %v", err)
			continue
		}

		server.waitGroup.Add(1)
		go func() {
			defer server.waitGroup.Done()
			core.IncConnections()
			server.handleConnection(conn)
			core.DecConnections()
		}()
	}
}

func (server *Server) Close(shutdownContext context.Context) error {
	if server.listener != nil {
		_ = server.listener.Close()
	}
	if server.shutdownChan != nil {
		close(server.shutdownChan)
	}

	done := make(chan struct{})
	go func() {
		server.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-shutdownContext.Done():
	}

	server.saveRDBSnapshot()
	server.closePersistence()
	return nil
}

func (server *Server) saveRDBSnapshot() {
	if server.rdb == nil || !config.Global.Persistence.RDB.Enabled {
		return
	}

	snapshot := persistence.Snapshot{
		DictData: server.ctx.Store.Dictionary.Snapshot(),
		SetData:  server.ctx.Store.Set.Snapshot(),
		ListData: server.ctx.Store.List.Snapshot(),
		HashData: server.ctx.Store.Hash.Snapshot(),
	}
	_ = server.rdb.Save(snapshot, config.Global.Persistence.RDB.Filename)
}

func (server *Server) closePersistence() {
	if server.aof != nil {
		_ = server.aof.Close()
	}
	if server.rdb != nil {
		_ = server.rdb.Close()
	}
}

func (server *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	connContext := core.NewConnContext(server.ctx)
	defer core.UnwatchConn(connContext.ID)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authenticated := config.Global.Server.Auth == ""

	for {
		_ = conn.SetReadDeadline(time.Now().Add(config.Global.ReadTimeout()))

		request, err := protocol.Decode(reader)
		if err != nil {
			return
		}

		cmdName := server.extractCommandName(request)

		if !authenticated {
			if server.handleUnauthenticated(conn, writer, request, cmdName, &authenticated, connContext) {
				continue
			}
		}

		core.MonitorPublish(cmdName, request.Array[1:])
		response := core.Execute(connContext, cmdName, request.Array[1:])
		core.IncCommands()

		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, conn, response); err != nil {
			return
		}

		if cmdName == "SUBSCRIBE" {
			server.enterPubsubMode(conn, writer)
			return
		}

		if cmdName == "MONITOR" {
			server.enterMonitorMode(conn, writer)
			return
		}
	}
}

func (server *Server) extractCommandName(request protocol.Value) string {
	if request.Type == protocol.TypeArray && len(request.Array) > 0 {
		return strings.ToUpper(request.Array[0].String)
	}
	return ""
}

func (server *Server) handleUnauthenticated(conn net.Conn, writer *bufio.Writer, request protocol.Value, cmdName string, authenticated *bool, connContext *core.ConnContext) bool {
	switch cmdName {
	case "AUTH":
		response := core.Execute(connContext, cmdName, request.Array[1:])
		if response.Type == protocol.TypeSimpleString && response.String == "OK" {
			*authenticated = true
		}
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		server.writeResponse(writer, conn, response)
		return true

	case "PING", "QUIT":
		return false

	default:
		response := protocol.Value{Type: protocol.TypeError, String: "NOAUTH Authentication required."}
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		server.writeResponse(writer, conn, response)
		return true
	}
}

func (server *Server) enterPubsubMode(conn net.Conn, writer *bufio.Writer) {
	msgChan := core.GetActiveChannel()
	if msgChan == nil {
		return
	}

	for msg := range msgChan {
		response := protocol.Value{
			Type: protocol.TypeArray,
			Array: []protocol.Value{
				{Type: protocol.TypeBulkString, String: "message"},
				{Type: protocol.TypeBulkString, String: msg.Channel},
				{Type: protocol.TypeBulkString, String: msg.Content},
			},
		}
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, conn, response); err != nil {
			return
		}
	}
}

func (server *Server) enterMonitorMode(conn net.Conn, writer *bufio.Writer) {
	monitorChannel := core.MonitorSubscribe()
	defer core.MonitorUnsubscribe(monitorChannel)

	for message := range monitorChannel {
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, conn, message); err != nil {
			return
		}
	}
}

func (server *Server) writeResponse(writer *bufio.Writer, conn net.Conn, value protocol.Value) error {
	if _, err := writer.WriteString(protocol.Encode(value)); err != nil {
		log.Printf("%s write error: %v", conn.RemoteAddr(), err)
		return err
	}
	if err := writer.Flush(); err != nil {
		log.Printf("%s flush error: %v", conn.RemoteAddr(), err)
		return err
	}
	return nil
}
