package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"

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

	log.Printf("listening on %s", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		req, err := resp.Decode(reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("%s disconnected", conn.RemoteAddr())
				return
			}

			log.Printf("%s decode error: %v", conn.RemoteAddr(), err)
			return
		}

		if req.Type != resp.ARRAY || len(req.Array) == 0 {
			v := resp.Value{Type: resp.ERROR, Str: "ERR protocol error"}
			if _, werr := writer.WriteString(resp.Encode(v)); werr != nil {
				log.Printf("%s write error: %v", conn.RemoteAddr(), werr)
				return
			}
			if ferr := writer.Flush(); ferr != nil {
				log.Printf("%s flush error: %v", conn.RemoteAddr(), ferr)
				return
			}
			continue
		}

		cmd := strings.ToUpper(req.Array[0].Str)
		handler, ok := command.Lookup(cmd)

		if !ok {
			v := resp.Value{
				Type: resp.ERROR,
				Str:  "ERR unknown command",
			}
			if _, werr := writer.WriteString(resp.Encode(v)); werr != nil {
				log.Printf("%s write error: %v", conn.RemoteAddr(), werr)
				return
			}
			if ferr := writer.Flush(); ferr != nil {
				log.Printf("%s flush error: %v", conn.RemoteAddr(), ferr)
				return
			}
			continue
		}

		args := req.Array[1:]
		result := handler(args)
		if _, werr := writer.WriteString(resp.Encode(result)); werr != nil {
			log.Printf("%s write error: %v", conn.RemoteAddr(), werr)
			return
		}
		if ferr := writer.Flush(); ferr != nil {
			log.Printf("%s flush error: %v", conn.RemoteAddr(), ferr)
			return
		}
	}
}
