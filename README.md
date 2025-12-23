# ValkeyDB

A high-performance, Redis-compatible in-memory database built from scratch in Go.

## Key Features

- **RESP Protocol Compliant**: Fully compatible with redis-cli and other Redis clients
- **Multiple Data Structures**:
  - Dictionary (String key-value pairs)
  - Sets (Unique collections)
  - Lists (Deque semantics)
  - Hashes (Field-value maps)
  - Sorted Sets (Score-based ordering using Skip List)
  - Pub/Sub (Message broadcasting)
- **Memory Eviction**: Configurable eviction strategies (LRU, LFU, EvictFirst)
- **Dual Persistence**: AOF and RDB support
- **TTL Support**: Automatic key expiration

## Getting Started

```bash
git clone https://github.com/william1nguyen/valkeydb.git
cd valkeydb
make build
make run
```

Connect with redis-cli:

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello"
OK
127.0.0.1:6379> GET mykey
"Hello"
```

## Supported Commands

| Category | Commands |
|----------|----------|
| String | SET, GET, DEL, TTL, EXPIRE, PING |
| Sorted Set | ZADD, ZREM, ZSCORE, ZRANK, ZCARD, ZRANGE |
| Set | SADD, SREM, SCARD, SMEMBERS, SISMEMBER, SEXPIRE, STTL |
| List | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, SORT |
| Hash | HSET, HGET, HDEL, HGETALL, HEXISTS, HLEN |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PUBLISH |
| System | AUTH, INFO, BGSAVE, KEYS, MONITOR |

## Configuration

Edit `config.yaml`:

```yaml
server:
  addr: ":6379"
  read_timeout: 300
  write_timeout: 300
  auth: secretpassword

persistence:
  aof:
    enabled: true
    filename: "appendonly.aof"
    rewrite_interval: 60
  rdb:
    enabled: true
    filename: "dump.rdb"

datastructure:
  expiration:
    max_sample_size: 20
    max_sample_rounds: 3
    check_interval: 1

memory:
  key_limit: 5000000     # not set is unlimited
  evict_strategy: "lru"  # lru, lfu, evict_first

logging:
  level: "info"
  verbose_persistence: true
```

### Memory Eviction

When `key_limit` is exceeded, keys are evicted based on `evict_strategy`:

| Strategy | Behavior |
|----------|----------|
| `lru` | Evict least recently accessed key |
| `lfu` | Evict key with lowest access frequency |
| `evict_first` | Evict earliest inserted key |

## Architecture

```
valkeydb/
├── cmd/valkeydb/          # Entry point
├── core/
│   ├── storage/           # Pure data structures
│   └── store/             # Store interface
├── command/               # Self-registering commands
├── protocol/              # RESP protocol
├── persistence/           # AOF/RDB
├── server/                # TCP server
└── config/                # Configuration
```

## Docker

```bash
docker build -t valkeydb:latest .
docker run --rm -p 6379:6379 valkeydb:latest
```

## TODO

- [x] RESP protocol
- [x] Dictionary, Set, List, Hash
- [x] Sorted Sets (ZADD, ZRANGE, ZRANK)
- [x] AOF and RDB persistence
- [x] TTL and key expiration
- [x] Memory eviction (LRU/LFU)
- [ ] Transaction support (MULTI/EXEC)
- [ ] Replication
- [ ] Cluster mode