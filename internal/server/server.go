package server

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/william1nguyen/valkeydb/internal/command"
	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

type Server struct {
	addr     string
	listener net.Listener
	dict     *datastructure.Dict
	set      *datastructure.Set
	pubsub   *datastructure.Pubsub
	aof      *persistence.AOF
	rdb      *persistence.RDB
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func New(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) ListenAndServe() error {
	if err := s.initialize(); err != nil {
		return err
	}

	s.startBackgroundTasks()
	log.Printf("listening on %s", s.addr)

	return s.acceptLoop()
}

func (s *Server) initialize() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	s.stopCh = make(chan struct{})

	s.dict = datastructure.CreateDict()
	s.set = datastructure.CreateSet()
	s.pubsub = datastructure.CreatePubsub()

	aofFile := config.Global.Persistence.AOF.Filename
	rdbFile := config.Global.Persistence.RDB.Filename

	if s.aof, err = persistence.OpenAOF(aofFile, config.Global.Persistence.AOF.Enabled); err != nil {
		return err
	}
	if s.rdb, err = persistence.OpenRDB(rdbFile, config.Global.Persistence.RDB.Enabled); err != nil {
		return err
	}

	command.Init(&command.DB{Dict: s.dict, Set: s.set, Pubsub: s.pubsub, AOF: s.aof, RDB: s.rdb})

	s.loadRDB()
	s.loadAOF()

	return nil
}

func (s *Server) loadRDB() {
	rdbFile := config.Global.Persistence.RDB.Filename
	snapshot, err := s.rdb.Load(rdbFile)
	if err != nil {
		log.Printf("RDB load error: %v", err)
		return
	}
	if snapshot == nil {
		return
	}

	for key, item := range snapshot.DictData {
		s.dict.Set(key, item.Value, 0)
		if !item.ExpiredAt.IsZero() {
			s.dict.ExpireAt(key, item.ExpiredAt)
		}
	}

	for key, item := range snapshot.SetData {
		if len(item.Members) > 0 {
			members := make([]string, 0, len(item.Members))
			for m := range item.Members {
				members = append(members, m)
			}
			s.set.Sadd(key, members...)
			if !item.ExpiredAt.IsZero() {
				s.set.ExpireAt(key, item.ExpiredAt)
			}
		}
	}

	log.Printf("RDB loaded: %d dict keys, %d set keys", len(snapshot.DictData), len(snapshot.SetData))
}

func (s *Server) loadAOF() {
	aofFile := config.Global.Persistence.AOF.Filename
	s.aof.Load(aofFile, func(cmd string, args []resp.Value) {
		command.Replay(cmd, args)
	})
}

func (s *Server) startBackgroundTasks() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(config.Global.GetAOFRewriteInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.rewriteAOF()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *Server) rewriteAOF() {
	combined := make(map[string]datastructure.Item)
	for k, v := range s.dict.Dump() {
		combined[k] = v
	}
	for k, v := range s.set.Dump() {
		combined[k] = v
	}

	aofFile := config.Global.Persistence.AOF.Filename
	if err := s.aof.Rewrite(func() map[string]datastructure.Item {
		return combined
	}, aofFile); err != nil {
		log.Printf("aof rewrite error: %v", err)
	} else {
		log.Printf("aof rewrite done")
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil
			default:
			}
			log.Printf("accept error: %v", err)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) Close(ctx context.Context) error {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.stopCh != nil {
		close(s.stopCh)
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	if s.aof != nil {
		_ = s.aof.Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		_ = conn.SetReadDeadline(time.Now().Add(config.Global.GetReadTimeout()))
		req, err := s.readRequest(reader, conn)
		if err != nil {
			return
		}

		respVal := s.dispatchCommand(req)
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.GetWriteTimeout()))
		if err := s.writeResponse(writer, conn, respVal); err != nil {
			return
		}

		if req.Type == resp.Array && len(req.Items) > 0 {
			cmd := strings.ToUpper(req.Items[0].Text)
			if cmd == "SUBSCRIBE" {
				s.pubsubMode(conn, writer)
				return
			}
		}
	}
}

func (s *Server) pubsubMode(conn net.Conn, writer *bufio.Writer) {
	msgChan := command.GetSubChannel()
	if msgChan == nil {
		return
	}

	for msg := range msgChan {
		_ = conn.SetWriteDeadline(time.Now().Add(config.Global.GetWriteTimeout()))
		if err := s.writeResponse(writer, conn, msg); err != nil {
			return
		}
	}
}

func (s *Server) readRequest(r *bufio.Reader, conn net.Conn) (resp.Value, error) {
	req, err := resp.Decode(r)
	if err != nil {
		if err == io.EOF {
			log.Printf("%s disconnected", conn.RemoteAddr())
		} else {
			log.Printf("%s decode error: %v", conn.RemoteAddr(), err)
		}
		return resp.Value{}, err
	}
	return req, nil
}

func (s *Server) dispatchCommand(req resp.Value) resp.Value {
	if req.Type != resp.Array || len(req.Items) == 0 {
		return resp.Value{Type: resp.Error, Text: "ERR protocol error"}
	}

	cmd := strings.ToUpper(req.Items[0].Text)
	handler, ok := command.Lookup(cmd)
	if !ok {
		return resp.Value{Type: resp.Error, Text: "ERR unknown command"}
	}

	args := req.Items[1:]
	return handler(args)
}

func (s *Server) writeResponse(w *bufio.Writer, conn net.Conn, v resp.Value) error {
	if _, err := w.WriteString(resp.Encode(v)); err != nil {
		log.Printf("%s write error: %v", conn.RemoteAddr(), err)
		return err
	}
	if err := w.Flush(); err != nil {
		log.Printf("%s flush error: %v", conn.RemoteAddr(), err)
		return err
	}
	return nil
}
