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

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen(networkType, s.address)
	if err != nil {
		return err
	}

	s.listener = listener
	s.shutdownChan = make(chan struct{})

	s.startBackgroundTasks()
	log.Printf("listening on %s", s.address)

	return s.acceptConnections()
}

func (s *Server) startBackgroundTasks() {
	s.waitGroup.Add(1)
	go func() {
		defer s.waitGroup.Done()
		ticker := time.NewTicker(config.Global.AOFRewriteInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.rewriteAOF()
			case <-s.shutdownChan:
				return
			}
		}
	}()
}

func (s *Server) rewriteAOF() {
	aofFilename := config.Global.Persistence.AOF.Filename
	err := s.aof.RewriteAll(
		s.ctx.Store.Dictionary.Snapshot(),
		s.ctx.Store.Set.Snapshot(),
		s.ctx.Store.List.Snapshot(),
		s.ctx.Store.HashMap.Snapshot(),
		aofFilename,
	)
	if err != nil {
		log.Printf("aof rewrite error: %v", err)
		return
	}
	log.Printf("aof rewrite done")
}

func (s *Server) acceptConnections() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownChan:
				return nil
			default:
			}
			log.Printf("accept error: %v", err)
			continue
		}

		s.waitGroup.Add(1)
		go func() {
			defer s.waitGroup.Done()
			core.IncConnections()
			s.handleConnection(conn)
			core.DecConnections()
		}()
	}
}

func (s *Server) Close(c context.Context) error {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.shutdownChan != nil {
		close(s.shutdownChan)
	}

	done := make(chan struct{})
	go func() {
		s.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-c.Done():
	}

	s.saveRDBSnapshot()
	s.closePersistence()
	return nil
}

func (s *Server) saveRDBSnapshot() {
	if s.rdb == nil || !config.Global.Persistence.RDB.Enabled {
		return
	}

	snapshot := persistence.Snapshot{
		DictData: s.ctx.Store.Dictionary.Snapshot(),
		SetData:  s.ctx.Store.Set.Snapshot(),
		ListData: s.ctx.Store.List.Snapshot(),
		HashData: s.ctx.Store.HashMap.Snapshot(),
	}
	_ = s.rdb.Save(snapshot, config.Global.Persistence.RDB.Filename)
}

func (s *Server) closePersistence() {
	if s.aof != nil {
		_ = s.aof.Close()
	}
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authenticated := config.Global.Server.Auth == ""

	for {
		_ = conn.SetReadDeadline(time.Now().Add(config.Global.ReadTimeout()))

		request, err := protocol.Decode(reader)
		if err != nil {
			return
		}

		cmdName := s.extractCommandName(request)

		if !authenticated {
			if s.handleUnauthenticated(conn, writer, request, cmdName, &authenticated) {
				continue
			}
		}

		core.MonitorPublish(cmdName, request.Array[1:])
		response := core.Execute(s.ctx, cmdName, request.Array[1:])
		core.IncCommands()

		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := s.writeResponse(writer, conn, response); err != nil {
			return
		}

		if cmdName == "SUBSCRIBE" {
			s.enterPubsubMode(conn, writer)
			return
		}

		if cmdName == "MONITOR" {
			s.enterMonitorMode(conn, writer)
			return
		}
	}
}

func (s *Server) extractCommandName(request protocol.Value) string {
	if request.Type == protocol.TypeArray && len(request.Array) > 0 {
		return strings.ToUpper(request.Array[0].String)
	}
	return ""
}

func (s *Server) handleUnauthenticated(conn net.Conn, writer *bufio.Writer, request protocol.Value, cmdName string, authenticated *bool) bool {
	switch cmdName {
	case "AUTH":
		response := core.Execute(s.ctx, cmdName, request.Array[1:])
		if response.Type == protocol.TypeSimpleString && response.String == "OK" {
			*authenticated = true
		}
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		s.writeResponse(writer, conn, response)
		return true

	case "PING", "QUIT":
		return false

	default:
		response := protocol.Value{Type: protocol.TypeError, String: "NOAUTH Authentication required."}
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		s.writeResponse(writer, conn, response)
		return true
	}
}

func (s *Server) enterPubsubMode(conn net.Conn, writer *bufio.Writer) {
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
		if err := s.writeResponse(writer, conn, response); err != nil {
			return
		}
	}
}

func (s *Server) enterMonitorMode(conn net.Conn, writer *bufio.Writer) {
	monitorChan := core.MonitorSubscribe()
	defer core.MonitorUnsubscribe(monitorChan)

	for msg := range monitorChan {
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := s.writeResponse(writer, conn, msg); err != nil {
			return
		}
	}
}

func (s *Server) writeResponse(writer *bufio.Writer, conn net.Conn, value protocol.Value) error {
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
