package main

import (
	"log"

	"github.com/william1nguyen/valkeydb/internal/server"
)

func main() {
	if err := server.ListenAndServe(":6379"); err != nil {
		log.Fatal(err)
	}
}
