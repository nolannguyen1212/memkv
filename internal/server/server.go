package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/william1nguyen/valkeydb/internal/aof"
	"github.com/william1nguyen/valkeydb/internal/command"
	"github.com/william1nguyen/valkeydb/internal/resp"
	"github.com/william1nguyen/valkeydb/internal/store"
)

type Server struct {
	addr string
}

func New(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	defer listener.Close()

	mem := store.NewMemoryStore()
	aofHandler, err := aof.Open("appendonly.aof", true)

	if err != nil {
		return err
	}

	command.Init(mem, aofHandler)

	aofHandler.Load("appendonly.aof", func(cmd string, args []resp.Value) {
		command.Replay(cmd, args)
	})

	go func() {
		for {
			time.Sleep(60 * time.Second)
			if err := aofHandler.Rewrite(func() map[string]store.Entry {
				return mem.Dump()
			}, "appendonly.aof"); err != nil {
				log.Printf("aof rewrite error: %v", err)
			} else {
				log.Printf("aof rewrite done")
			}
		}
	}()

	log.Printf("listening on %s", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		req, err := s.readRequest(reader, conn)
		if err != nil {
			return
		}

		respVal := s.dispatchCommand(req)
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
