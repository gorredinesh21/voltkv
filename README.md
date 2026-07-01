# voltkv

A **Redis-compatible in-memory key-value store**, written from scratch in Go. It speaks the
real Redis RESP2 wire protocol, so you can talk to it with `redis-cli`, `redis-benchmark`,
and standard Redis client libraries.

Built to demonstrate TCP networking, protocol parsing, and safe concurrency under thousands
of simultaneous client connections.

## Quick start

```bash
go run ./cmd/server                        # starts on :6380 with 16 shards
go run ./cmd/server -aof voltkv.aof        # with append-only-file persistence

# in another terminal (requires redis-tools installed):
redis-cli -p 6380 PING              # -> PONG
redis-cli -p 6380 SET foo bar       # -> OK
redis-cli -p 6380 GET foo           # -> "bar"
redis-cli -p 6380 SET tmp x EX 5    # expires in 5s
redis-cli -p 6380 DEL foo           # -> (integer) 1
```

### Flags

| flag          | default   | meaning                                            |
|---------------|-----------|----------------------------------------------------|
| `-addr`       | `:6380`   | listen address (binds IPv6 `::` on Windows)        |
| `-shards`     | `16`      | keyspace shards (rounded up to a power of two)     |
| `-sweep`      | `1s`      | background TTL sweep interval                      |
| `-aof`        | `""`      | append-only file path; empty disables persistence  |
| `-maxclients` | `0`       | max concurrent clients (0 = unlimited)             |

### Windows note

The portable Go SDK isn't on PATH on the dev box, and the server binds IPv6 `::`.
Use the task runner and connect over `::1`:

```powershell
.\make.ps1 build          # -> voltkv.exe (static, CGO disabled)
.\make.ps1 test
.\make.ps1 run            # :6380 with AOF
# smoke-test with a raw TCP client to ::1 (not 127.0.0.1)
```

## Run with Docker

```bash
docker build -t voltkv .
docker run -p 6380:6380 voltkv
# with persistence on a mounted volume:
docker run -p 6380:6380 -v voltkv-data:/data voltkv -addr :6380 -aof /data/voltkv.aof
```

## Prove the concurrency

```bash
# 200 concurrent clients, 100k ops — your headline throughput number
redis-benchmark -p 6380 -t set,get -c 200 -n 100000 -q

# correctness under contention (needs a C toolchain / cgo for -race):
go test -race ./...
# on machines without a C compiler, run plain:
go test -timeout 60s ./...

# why sharding matters (paste this table into the README):
go test -bench BenchmarkSetShards -benchmem ./internal/store
```

### Measured: why sharding matters

`BenchmarkSetShards` on a Ryzen 5 Pro 7535U (12 threads), parallel writers — more shards,
less lock contention:

| Shards | ns/op | vs 1 shard |
|-------:|------:|-----------:|
| 1  | 229.3 | 1× |
| 4  | 119.5 | ~1.9× |
| 16 | 56.7  | ~4× |
| 64 | 36.5  | **~6.3×** |

## Design

- **Goroutine-per-connection** TCP server with `context`-based graceful shutdown.
- **Sharded keyspace (lock striping):** N shards each with their own `sync.RWMutex`, so
  writes to different keys don't contend.
- **TTL expiration:** lazy (on read) + active (background sweeper goroutine).

See [SPEC.md](SPEC.md) for the full architecture, roadmap, and resume bullets.

## Supported commands

**Connection**
`PING [msg]` · `ECHO msg` · `QUIT`

**Strings**
`SET key val [EX s | PX ms] [NX | XX]` · `GET` · `GETSET` · `APPEND` · `STRLEN` ·
`INCR` · `DECR` · `INCRBY` · `DECRBY` · `MGET` · `MSET`

**Generic keys**
`DEL` · `EXISTS` · `TYPE` · `EXPIRE` · `PEXPIRE` · `TTL` · `PTTL` · `PERSIST` ·
`KEYS pattern` (glob: `*`, `?`, `[abc]`, `[a-z]`, `[^x]`) · `DBSIZE` · `FLUSHDB`

**Hashes**
`HSET` · `HGET` · `HDEL` · `HGETALL` · `HKEYS` · `HVALS` · `HLEN` · `HEXISTS`

**Lists**
`LPUSH` · `RPUSH` · `LPOP` · `RPOP` · `LRANGE` · `LLEN` · `LINDEX`

**Pub/Sub**
`SUBSCRIBE ch [ch...]` · `UNSUBSCRIBE [ch...]` · `PUBLISH ch msg`

**Server**
`INFO` (uptime, connected clients, total commands, keyspace size, AOF status) ·
`COMMAND`

### Persistence (AOF)

With `-aof <path>`, every write command is serialized to RESP and appended to the
file by a single writer goroutine (fed over a channel — no per-command locking).
On startup the file is replayed to rebuild the exact dataset. `Rewrite` compacts
the log to the minimum commands that recreate current state (snapshot → temp file
→ atomic rename).

### Types are enforced

Each key holds exactly one type (string / hash / list). Using the wrong-type
command (e.g. `LPUSH` on a string) returns a Redis-style `WRONGTYPE` error.
