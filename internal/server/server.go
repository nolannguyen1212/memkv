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

	"github.com/william1nguyen/valkeydb/internal/aof"
	"github.com/william1nguyen/valkeydb/internal/command"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Server struct {
	addr     string
	listener net.Listener
	mem      *store.MemoryStore
	aof      *aof.AOF
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

	mem := store.NewMemoryStore()
	aofHandler, err := aof.Open("appendonly.aof", true)

	if err != nil {
		return err
	}

	command.Init(mem, aofHandler)

	aofHandler.Load("appendonly.aof", func(cmd string, args []resp.Value) {
		command.Replay(cmd, args)
	})

	s.mem = mem
	s.aof = aofHandler
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := aofHandler.Rewrite(func() map[string]store.Entry {
					return mem.Dump()
				}, "appendonly.aof"); err != nil {
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
	if s.mem != nil {
		s.mem.Close()
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
	if req.Type != resp.ARRAY || len(req.Array) == 0 {
		return resp.Value{
			Type: resp.ERROR,
			Str:  "ERR protocol error",
		}
	}

	cmd := strings.ToUpper(req.Array[0].Str)
	handler, ok := command.Lookup(cmd)
	if !ok {
		return resp.Value{Type: resp.ERROR, Str: "ERR unknown command"}
	}

	args := req.Array[1:]
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
