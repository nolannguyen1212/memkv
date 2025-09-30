package main

import (
	"log"

	"github.com/william1nguyen/valkeydb/internal/config"
	"github.com/william1nguyen/valkeydb/internal/server"
)

var (
	configFile = "config.yaml"
)

func main() {
	if err := config.Load(configFile); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	s := server.New(config.Global.Server.Addr)
	log.Printf("Starting ValkeyDB on %s", config.Global.Server.Addr)
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
