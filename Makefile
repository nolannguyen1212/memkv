.PHONY: build run test lint

build:
	go build -o bin/valkeydb ./cmd/valkeydb

run:
	go run ./cmd/valkeydb

test:
	go test ./...
