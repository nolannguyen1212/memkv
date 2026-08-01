.PHONY: build run replica test clean

CONFIG ?= config.yaml
REPLICA_CONFIG ?= config.replica.yaml

build:
	@go build -o bin/valkeydb ./cmd/valkeydb

run:
	@go run ./cmd/valkeydb --config=$(CONFIG)

replica:
	@go run ./cmd/valkeydb --config=$(REPLICA_CONFIG)

test:
	@go test ./...

test-v:
	@go test -v ./...

test-cover:
	@go test -cover ./...
	@go test -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out

clean:
	@rm -rf bin/ coverage.out *.aof *.rdb test_*
