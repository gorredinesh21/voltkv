# voltkv — Handover

A working-status handover for anyone picking this project up (including future me). It
explains the goal, what is done and verified, how to run it, and what remains.

---

## 1. What we set out to build

A **Redis-compatible in-memory key-value store, written from scratch in Go** — a portfolio
project chosen to demonstrate three things employers actually probe:

1. **Systems fundamentals** — TCP networking and implementing a real wire protocol (RESP2),
   not just calling a library.
2. **Concurrency done correctly** — safely serving many simultaneous clients and a shared
   keyspace, which is Go's core strength.
3. **Durability** — surviving restarts via on-disk persistence.

The bar was: *a real Redis client (`redis-cli`, `redis-benchmark`) should be able to talk to
it unmodified*, and the concurrency benefit should be **measurable**, not just claimed.

---

## 2. What we built (done & verified ✅)

Everything below compiles (`go build ./...`), passes `go vet`, passes `go test`, and was
confirmed with a live smoke test over a real TCP socket.

**Protocol & server**
- RESP2 wire-protocol reader/writer (`internal/resp`) — interoperable with real Redis clients.
- TCP server using the goroutine-per-connection model with `context`-based graceful shutdown
  and an optional max-clients cap (`internal/server`).

**Storage engine (`internal/store`)**
- Sharded, lock-striped keyspace (each shard has its own `sync.RWMutex`) so writes to
  different keys don't contend. **Measured ~6.3× throughput improvement** going from 1 → 64
  shards (`BenchmarkSetShards`).
- Type-tagged values: strings, hashes, and lists, with `WRONGTYPE` enforcement.
- TTL expiry: lazy (on read) + active (background sweeper goroutine).

**Commands implemented**
- Strings/counters: `SET` (with `EX`/`PX`/`NX`/`XX`), `GET`, `GETSET`, `DEL`, `EXISTS`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`.
- Keys: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `KEYS` (glob), `FLUSHDB`,
  `DBSIZE`.
- Hashes: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`.
- Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`.
- Server: `PING`, `ECHO`, `INFO`.

**Pub/Sub (`internal/pubsub`)**
- `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` with goroutine + channel fan-out. Publishing uses
  non-blocking sends so one slow subscriber can't stall the publisher.

**Persistence — AOF (`internal/aof`)**
- Append-only file with a single writer goroutine (fed by a channel, off the hot path).
- Replay on startup rebuilds the full keyspace.
- `Rewrite` compaction: snapshot current state to a temp file and atomically swap it in.
- **Verified across a real restart:** killed the process, started a new one against the same
  file, and all keys/values (string counter, hash, list) came back; `DBSIZE` matched.

**Ops & docs**
- Multi-stage `Dockerfile` (static `CGO_ENABLED=0` binary → scratch image).
- `Makefile` + `make.ps1` (build/test/run/bench/vet/clean/docker).
- Tests: `internal/store` (types, expiry, glob, concurrency, shard benchmark) and
  `internal/aof` (replay across simulated restart, rewrite, pub/sub).
- `README.md` documents all commands, flags, and Docker usage.

---

## 3. How to build & run

Requires the Go toolchain (Go 1.23+).

```bash
go build ./...
go test ./...
go run ./cmd/server -addr :6380 -shards 16 -aof voltkv.aof
```

Then, from any Redis client:

```bash
redis-cli -p 6380 PING            # -> PONG
redis-cli -p 6380 SET foo bar     # -> OK
redis-cli -p 6380 HSET u name dinesh
redis-cli -p 6380 INFO
```

**Flags:** `-addr` (listen address), `-shards` (keyspace shards), `-sweep` (TTL sweep
interval), `-aof` (append-only file path; empty = persistence disabled), `-maxclients`.

**Notes for the reader**
- On Windows, `:6380` binds to the IPv6 loopback — connect via `::1` (not `127.0.0.1`) if you
  hit "connection refused" during local testing.
- The `-race` detector needs a C compiler (cgo). If none is available, run the normal test
  suite instead; correctness is still covered.

---

## 4. What is pending / next steps

Intentionally **out of scope for v1.0** (documented in SPEC.md), listed roughly by value:

- **RDB-style snapshotting** (point-in-time binary dump) alongside AOF.
- **Eviction / max-memory policies** (LRU/LFU) — currently the store grows unbounded.
- **Transactions** (`MULTI`/`EXEC`/`WATCH`).
- **More commands**: `SETNX`, `SETEX`, `RENAME`, `SCAN` (cursor), `LINSERT`/`LSET`, sets
  (`SADD`/`SMEMBERS`), sorted sets (`ZADD`/`ZRANGE`).
- **RESP3 protocol** support.
- **AUTH + TLS** for secured connections.
- **Replication / clustering** (the big one — primary/replica, `WAIT`).
- **A `/metrics` (Prometheus) endpoint** in addition to `INFO`.

No known bugs in the implemented feature set.

---

## 5. Repo layout

```
voltkv/
  cmd/server/           entrypoint: flags, listener, signal handling
  internal/server/      accept loop, per-connection handler, INFO stats
  internal/resp/        RESP2 reader/writer
  internal/store/       sharded, typed, TTL-aware key-value store (+ tests)
  internal/command/     command dispatch table
  internal/pubsub/      publish/subscribe broker
  internal/aof/         append-only-file persistence (+ tests)
  Dockerfile, Makefile, make.ps1
  README.md, SPEC.md, HANDOVER.md
```
