package main

import (
	"log"

	"github.com/william1nguyen/valkeydb/internal/server"
)

func main() {
	s := server.New(":6379")
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
