package server

import (
	"bufio"
	"fmt"
	"net"

	"github.com/william1nguyen/valkeydb/internal/command"
	"github.com/william1nguyen/valkeydb/internal/resp"
)

func ListenAndServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	fmt.Printf("Server listening on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
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
		v, err := resp.Read(reader)

		if err != nil {
			return
		}

		if v.Type != resp.ARRAY || len(v.Array) == 0 {
			_ = resp.Write(writer, resp.Value{
				Type: resp.ERROR,
				Str:  "ERR invalid command",
			})
			_ = writer.Flush()
			continue
		}

		cmd := v.Array[0].Str
		args := v.Array[1:]

		if h, ok := command.Lookup(cmd); ok {
			reply := h(args)
			_ = resp.Write(writer, reply)
		} else {
			_ = resp.Write(writer, resp.Value{
				Type: resp.ERROR,
				Str:  "ERR unknown command",
			})
		}

		if err := writer.Flush(); err != nil {
			return
		}
	}
}
