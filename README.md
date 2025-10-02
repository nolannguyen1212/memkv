# ValkeyDB

A lightweight, Redis-compatible in-memory database written in Go.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Using with Redis CLI](#using-with-redis-cli)
- [Supported Commands](#supported-commands)
  - [Dictionary Commands](#dictionary-commands-string-operations)
  - [Set Commands](#set-commands)
  - [Pub/Sub Commands](#pubsub-commands)
  - [System Commands](#system-commands)
- [Configuration](#configuration)
- [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Key Design Decisions](#key-design-decisions)
- [Testing](#testing)
- [Development](#development)
- [Performance Considerations](#performance-considerations)
- [TODO](#todo)

## Overview

ValkeyDB is a high-performance, Redis-compatible in-memory database implementation built from scratch in Go. It implements the RESP (REdis Serialization Protocol) and provides essential data structures with persistence capabilities. This project serves as both a learning resource for understanding database internals and a lightweight alternative for development environments.

## Features

### Core Capabilities

- **RESP Protocol**: Full Redis Serialization Protocol implementation for compatibility with standard Redis clients ([protocol/resp/resp.go](internal/protocol/resp/resp.go))
- **Multiple Data Structures**: 
  - Dictionary (String key-value pairs) - [datastructure/dict.go](internal/datastructure/dict.go)
  - Sets (Unique collections) - [datastructure/set.go](internal/datastructure/set.go)
  - Lists (Deque semantics; LPUSH, RPUSH, LPOP, RPOP, LRANGE, SORT) - [datastructure/list.go](internal/datastructure/list.go)
  - Hashes (HSET multi field-value, HGET, HDEL, HGETALL, HEXISTS, HLEN) - [datastructure/hashmap.go](internal/datastructure/hashmap.go)
  - Pub/Sub (Message broadcasting) - [datastructure/pubsub.go](internal/datastructure/pubsub.go)
- **Dual Persistence**:
  - AOF (Append-Only File): Write-ahead logging with automatic rewrite; includes dict, set, list (RPUSH), hash (HSET) - [persistence/aof.go](internal/persistence/aof.go)
  - RDB (Redis Database): Point-in-time snapshots with background saving; includes dict, set, list, hash - [persistence/rdb.go](internal/persistence/rdb.go)
- **TTL Support**: Automatic key expiration with both passive and active expiration strategies
- **Concurrent Access**: Thread-safe operations with efficient read-write locking mechanisms
- **Configurable**: YAML-based configuration for all server settings - [config.yaml](config.yaml)

## Quick Start

### Prerequisites
- Go 1.25.1 or higher

### Installation

```bash
# Clone the repository
git clone https://github.com/william1nguyen/valkeydb.git
cd valkeydb

# Build the project
make build

# Run the server
make run
```

The server will start on `localhost:6379` by default.

### Using with Redis CLI

```bash
# Connect using redis-cli
redis-cli -p 6379

# Try some commands
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello ValkeyDB"
OK
127.0.0.1:6379> GET mykey
"Hello ValkeyDB"
```

## Supported Commands

See [command/registry.go](internal/command/registry.go) for the complete command registry implementation.

### Dictionary Commands (String Operations)

Implementation: [command/dict_command.go](internal/command/dict_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `SET key value [ttl]` | Set a key-value pair with optional TTL | `SET name "John" 60` |
| `GET key` | Get value by key | `GET name` |
| `DEL key [key ...]` | Delete one or more keys | `DEL name age` |
| `EXPIRE key seconds` | Set expiration time | `EXPIRE name 60` |
| `TTL key` | Get remaining time to live | `TTL name` |
| `PEXPIREAT key milliseconds` | Set expiration timestamp | `PEXPIREAT name 1735567200000` |
| `PING [message]` | Test connection | `PING` |

### Set Commands

Implementation: [command/set_command.go](internal/command/set_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `SADD key member [member ...]` | Add members to a set | `SADD myset "a" "b" "c"` |
| `SREM key member [member ...]` | Remove members from a set | `SREM myset "a"` |
| `SMEMBERS key` | Get all members of a set | `SMEMBERS myset` |
| `SISMEMBER key member` | Check if member exists | `SISMEMBER myset "a"` |
| `SCARD key` | Get set cardinality | `SCARD myset` |
| `SEXPIRE key seconds` | Set expiration for a set | `SEXPIRE myset 60` |
| `STTL key` | Get TTL for a set | `STTL myset` |

### List Commands

Implementation: [command/list_command.go](internal/command/list_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `LPUSH key value [value ...]` | Push values to the head | `LPUSH mylist a b c` |
| `RPUSH key value [value ...]` | Push values to the tail | `RPUSH mylist a b c` |
| `LPOP key [count]` | Pop from head | `LPOP mylist 2` |
| `RPOP key [count]` | Pop from tail | `RPOP mylist 2` |
| `LRANGE key start stop` | Get a range | `LRANGE mylist 0 -1` |
| `SORT key [ASC\|DESC] [ALPHA]` | In-place sort list | `SORT mylist ASC` |

### Hash Commands

Implementation: [command/hash_command.go](internal/command/hash_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `HSET key field value [field value ...]` | Set one or more field-value pairs; returns count of new fields | `HSET myhash f1 v1 f2 v2` |
| `HGET key field` | Get value of a field | `HGET myhash f1` |
| `HDEL key field [field ...]` | Delete fields; returns count removed | `HDEL myhash f1 f2` |
| `HGETALL key` | Get all field-value pairs | `HGETALL myhash` |
| `HEXISTS key field` | Check if field exists | `HEXISTS myhash f1` |
| `HLEN key` | Number of fields | `HLEN myhash` |

### Pub/Sub Commands

Implementation: [command/pubsub_command.go](internal/command/pubsub_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `SUBSCRIBE channel` | Subscribe to a channel | `SUBSCRIBE news` |
| `UNSUBSCRIBE` | Unsubscribe from channel | `UNSUBSCRIBE` |
| `PUBLISH channel message` | Publish message to channel | `PUBLISH news "Hello"` |

### System Commands

Implementation: [command/system_command.go](internal/command/system_command.go)

| Command | Description | Example |
|---------|-------------|---------|
| `BGSAVE [filename]` | Background save to RDB | `BGSAVE` |
| `KEYS pattern` | Find keys matching pattern | `KEYS user:*` |

### Authentication

Optional per-connection gate requiring clients to authenticate before most commands.

| Command | Description | Example |
|---------|-------------|---------|
| `AUTH password` | Authenticate the connection | `AUTH secretpassword` |

- Allowed before auth: `AUTH`, `PING`, `QUIT`.
- If no password is configured, auth is disabled.

Enable in `config.yaml`:

```yaml
server:
  addr: ":6379"
  read_timeout: 300
  write_timeout: 300
  auth: secretpassword
```

Example:

```bash
redis-cli -p 6379
AUTH secretpassword
OK
SET a 1
GET a
```

## Configuration

Edit `config.yaml` to customize server behavior:

```yaml
server:
  addr: ":6379"              # Server listen address
  read_timeout: 300          # Connection read timeout (seconds)
  write_timeout: 300         # Connection write timeout (seconds)

persistence:
  aof:
    enabled: true            # Enable AOF persistence
    filename: "appendonly.aof"
    rewrite_interval: 60     # AOF rewrite interval (seconds)
  
  rdb:
    enabled: true            # Enable RDB snapshots
    filename: "dump.rdb"

datastructure:
  expiration:
    max_sample_size: 20      # Keys to sample per expiration round
    max_sample_rounds: 3     # Max sampling rounds per cycle
    check_interval: 1        # Expiration check interval (seconds)

logging:
  level: "info"              # Log level: debug, info, warn, error
  verbose_persistence: true  # Verbose persistence logging
```

## Architecture

### Project Structure

```
valkeydb/
├── cmd/valkeydb/          # Application entry point
├── internal/
│   ├── command/           # Command handlers (dict, set, list, hash, pubsub, system)
│   ├── config/            # Configuration management
│   ├── datastructure/     # Core data structures (Dict, Set, List, Hash, Pubsub)
│   ├── persistence/       # Persistence layer (AOF, RDB)
│   ├── protocol/resp/     # RESP protocol implementation
│   └── server/            # TCP server and connection handling
├── config.yaml            # Configuration file
└── Makefile              # Build automation
```

### Key Design Decisions

- **Concurrent Safety**: All data structures use `sync.RWMutex` for thread-safe operations
- **Expiration Strategy**: Hybrid approach with passive (on-access) and active (periodic sampling) expiration
- **Persistence**: Dual persistence with AOF for durability and RDB for fast restarts
- **Protocol**: Full RESP implementation for compatibility with existing Redis clients

## Testing

```bash
# Run all tests
make test

# Run tests with verbose output
make test-v

# Run tests with coverage
make test-cover
```

## Development

```bash
# Build binary
make build

# Run server
make run

# Clean build artifacts and data files
make clean
```

## Performance Considerations

- **Active Expiration**: Configurable sampling to balance CPU usage and memory
- **AOF Rewrite**: Automatic compaction to prevent unbounded file growth
- **Lock Granularity**: Read-write locks minimize contention for read-heavy workloads
- **Connection Pooling**: Each client connection runs in its own goroutine

## TODO

- [x] RESP protocol encoder/decoder
- [x] TCP server with concurrent connection handling
- [x] Dictionary data structure with TTL support
- [x] Set data structure with TTL support
- [x] Pub/Sub messaging system
- [x] AOF persistence with automatic rewrite
- [x] RDB snapshot persistence with background saving
- [x] Active and passive key expiration
- [x] Configuration management via YAML
- [x] Command registry and handler system
- [x] Connection timeouts
- [x] Pattern-based key matching (KEYS command)
- [x] Comprehensive test coverage
- [x] List data structure (LPUSH, RPUSH, LPOP, RPOP, LRANGE, SORT)
- [x] Hash data structure (HSET multi-field, HGET, HDEL, HGETALL, HEXISTS, HLEN)
- [ ] Sorted sets with scores (ZADD, ZRANGE, ZRANK)
- [ ] Transaction support (MULTI/EXEC/DISCARD)
- [x] Authentication (AUTH command)
- [ ] Pipelining for batch command execution
- [ ] Monitoring and INFO command for server statistics
- [ ] Replication (master-slave)
- [ ] Memory management with LRU/LFU eviction policies
- [ ] Cluster mode with distributed sharding
- [ ] Lua scripting support (EVAL/EVALSHA)
- [ ] Slow log for tracking slow commands
- [ ] Prometheus metrics export
- [ ] Docker support and containerization
- [ ] HTTP API alongside RESP protocol
- [ ] Benchmark suite for performance testing
- [ ] Admin dashboard (web UI)
- [ ] Geospatial indexing (GEOADD, GEORADIUS)