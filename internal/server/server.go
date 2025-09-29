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
	"github.com/william1nguyen/valkeydb/internal/datastructure"
	"github.com/william1nguyen/valkeydb/internal/persistence"
	"github.com/william1nguyen/valkeydb/internal/protocol/resp"
)

const (
	aofFile = "appendonly.aof"
)

type Server struct {
	addr     string
	listener net.Listener
	dict     *datastructure.Dict
	aof      *persistence.AOF
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func New(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = listener
	s.stopCh = make(chan struct{})

	s.dict = datastructure.CreateDict()
	s.aof, err = persistence.Open(aofFile, true)

	if err != nil {
		return err
	}

	command.Init(&command.DB{Dict: s.dict, AOF: s.aof})

	s.aof.Load(aofFile, func(cmd string, args []resp.Value) {
		command.Replay(cmd, args)
	})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.aof.Rewrite(func() map[string]datastructure.Item {
					return s.dict.Dump()
				}, aofFile); err != nil {
					log.Printf("aof rewrite error: %v", err)
				} else {
					log.Printf("aof rewrite done")
				}
			case <-s.stopCh:
				return
			}
		}
	}()

	log.Printf("listening on %s", s.addr)

	for {
		conn, err := listener.Accept()
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
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		req, err := s.readRequest(reader, conn)
		if err != nil {
			return
		}

		respVal := s.dispatchCommand(req)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Minute))
		if err := s.writeResponse(writer, conn, respVal); err != nil {
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
