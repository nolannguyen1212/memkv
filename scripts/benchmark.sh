#!/bin/sh

set -eu

benchmark_count=${BENCH_COUNT:-3}
benchmark_time=${BENCH_TIME:-500ms}

git rev-parse HEAD
go version
go env GOOS GOARCH
go test -run '^$' -bench . -benchmem -benchtime="$benchmark_time" -count="$benchmark_count" ./internal/resp ./internal/store ./internal/wal ./internal/snapshot ./internal/server
