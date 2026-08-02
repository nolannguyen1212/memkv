.PHONY: build run replica test bench clean

CONFIG ?= config.yaml
REPLICA_CONFIG ?= config.replica.yaml

build:
	@go build -o bin/memkv ./cmd/memkv

run:
	@go run ./cmd/memkv --config=$(CONFIG)

replica:
	@go run ./cmd/memkv --config=$(REPLICA_CONFIG)

test:
	@go test ./...

test-v:
	@go test -v ./...

test-cover:
	@go test -cover ./...
	@go test -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out

bench:
	@./scripts/benchmark.sh

clean:
	@rm -rf bin/ coverage.out *.wal *.mksp test_*
