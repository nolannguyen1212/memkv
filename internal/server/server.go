package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

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

	for {
		req, err := resp.Decode(reader)
		if err != nil {
			return
		}

		cmd := strings.ToUpper(req.Array[0].Str)
		handler, ok := command.Lookup(cmd)

		if !ok {
			v := resp.Value{
				Type: resp.ERROR,
				Str:  "ERR unkown command",
			}
			conn.Write([]byte(resp.Encode(v)))
			continue
		}

		args := req.Array[1:]
		result := handler(args)
		conn.Write([]byte(resp.Encode(result)))
	}
}
