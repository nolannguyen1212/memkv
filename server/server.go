package server

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/command"
	"github.com/william1nguyen/valkeydb/command/pubsubcmd"
	"github.com/william1nguyen/valkeydb/command/systemcmd"
	"github.com/william1nguyen/valkeydb/config"
	"github.com/william1nguyen/valkeydb/core/store"
	"github.com/william1nguyen/valkeydb/persistence"
	"github.com/william1nguyen/valkeydb/protocol"
)

const networkType = "tcp"

type Server struct {
	address      string
	listener     net.Listener
	dataStore    *store.Store
	aof          *persistence.AOF
	rdb          *persistence.RDB
	shutdownChan chan struct{}
	waitGroup    sync.WaitGroup
}

func New(address string, dataStore *store.Store, aof *persistence.AOF, rdb *persistence.RDB) *Server {
	return &Server{
		address:   address,
		dataStore: dataStore,
		aof:       aof,
		rdb:       rdb,
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
		server.dataStore.Dictionary.Snapshot(),
		server.dataStore.Set.Snapshot(),
		server.dataStore.List.Snapshot(),
		server.dataStore.HashMap.Snapshot(),
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
		connection, err := server.listener.Accept()
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
			systemcmd.IncrementConnections()
			server.handleConnection(connection)
			systemcmd.DecrementConnections()
		}()
	}
}

func (server *Server) Close(context context.Context) error {
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
	case <-context.Done():
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
		DictData: server.dataStore.Dictionary.Snapshot(),
		SetData:  server.dataStore.Set.Snapshot(),
		ListData: server.dataStore.List.Snapshot(),
		HashData: server.dataStore.HashMap.Snapshot(),
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

func (server *Server) handleConnection(connection net.Conn) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	isAuthenticated := config.Global.Server.Auth == ""

	for {
		_ = connection.SetReadDeadline(time.Now().Add(config.Global.ReadTimeout()))

		request, err := protocol.Decode(reader)
		if err != nil {
			return
		}

		commandName := server.extractCommandName(request)

		if !isAuthenticated {
			if server.handleUnauthenticated(connection, writer, request, commandName, &isAuthenticated) {
				continue
			}
		}

		server.publishToMonitor(request, commandName)
		response := command.Execute(server.dataStore, commandName, request.Array[1:])
		systemcmd.IncrementCommands()

		_ = connection.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, connection, response); err != nil {
			return
		}

		if commandName == "SUBSCRIBE" {
			server.enterPubsubMode(connection, writer)
			return
		}

		if commandName == "MONITOR" {
			server.enterMonitorMode(connection, writer)
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

func (server *Server) handleUnauthenticated(connection net.Conn, writer *bufio.Writer, request protocol.Value, commandName string, isAuthenticated *bool) bool {
	switch commandName {
	case "AUTH":
		response := command.Execute(server.dataStore, commandName, request.Array[1:])
		if response.Type == protocol.TypeSimpleString && response.String == "OK" {
			*isAuthenticated = true
		}
		_ = connection.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		server.writeResponse(writer, connection, response)
		return true

	case "PING", "QUIT":
		return false

	default:
		response := protocol.Value{Type: protocol.TypeError, String: "NOAUTH Authentication required."}
		_ = connection.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		server.writeResponse(writer, connection, response)
		return true
	}
}

func (server *Server) publishToMonitor(request protocol.Value, commandName string) {
	if request.Type != protocol.TypeArray || len(request.Array) == 0 {
		return
	}
	systemcmd.MonitorPublish(commandName, request.Array[1:])
}

func (server *Server) enterPubsubMode(connection net.Conn, writer *bufio.Writer) {
	messageChannel := pubsubcmd.GetActiveChannel()
	if messageChannel == nil {
		return
	}

	for message := range messageChannel {
		response := protocol.Value{
			Type: protocol.TypeArray,
			Array: []protocol.Value{
				{Type: protocol.TypeBulkString, String: "message"},
				{Type: protocol.TypeBulkString, String: message.Channel},
				{Type: protocol.TypeBulkString, String: message.Content},
			},
		}

		_ = connection.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, connection, response); err != nil {
			return
		}
	}
}

func (server *Server) enterMonitorMode(connection net.Conn, writer *bufio.Writer) {
	monitorChannel := systemcmd.MonitorSubscribe()
	defer systemcmd.MonitorUnsubscribe(monitorChannel)

	for message := range monitorChannel {
		_ = connection.SetWriteDeadline(time.Now().Add(config.Global.WriteTimeout()))
		if err := server.writeResponse(writer, connection, message); err != nil {
			return
		}
	}
}

func (server *Server) writeResponse(writer *bufio.Writer, connection net.Conn, value protocol.Value) error {
	if _, err := writer.WriteString(protocol.Encode(value)); err != nil {
		log.Printf("%s write error: %v", connection.RemoteAddr(), err)
		return err
	}

	if err := writer.Flush(); err != nil {
		log.Printf("%s flush error: %v", connection.RemoteAddr(), err)
		return err
	}

	return nil
}
