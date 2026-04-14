.PHONY: build run replica test clean

ADDRESS ?=
APIKEY  ?=

build:
	@go build -o bin/valkeydb ./cmd/valkeydb

run:
	@go run ./cmd/valkeydb

replica:
	@if [ -z "$(ADDRESS)" ]; then echo "Usage: make replica ADDRESS=<primary-addr> APIKEY=<primary-apikey>"; exit 1; fi
	@if [ -z "$(APIKEY)" ]; then echo "Usage: make replica ADDRESS=<primary-addr> APIKEY=<primary-apikey>"; exit 1; fi
	@go run ./cmd/valkeydb --join-addr=$(ADDRESS) --join-apikey=$(APIKEY)

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
