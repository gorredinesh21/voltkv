# voltkv — a Redis-compatible in-memory store (Go)

> **Elevator pitch:** An in-memory key-value store that speaks the real Redis wire
> protocol (RESP2), so you can talk to it with `redis-cli`, `redis-benchmark`, and any
> Redis client library. Built from scratch in Go to demonstrate TCP networking, protocol
> parsing, and safe concurrency under thousands of simultaneous connections.

This is the "rebuild a famous system" flagship. It is honestly concurrency-heavy: a real
server handles many client connections at once, and the shared keyspace must stay correct
under concurrent reads/writes. That's genuine Go concurrency you can *measure* on your own
laptop with `redis-benchmark` — no real users required.

---

## Why this is an expert-level project (and interview-proof)

- **Real protocol, not a toy API.** It implements RESP2, so `redis-cli -p 6380 SET foo bar`
  Just Works. You can point real tooling at it. That is instantly credible.
- **Concurrency you can demonstrate.** `redis-benchmark -p 6380 -c 200 -n 100000` simulates
  200 concurrent clients firing 100k commands. You show throughput numbers in your README.
- **Correctness under contention.** Sharded, lock-striped keyspace + TTL expiry. You can
  talk about data races, `sync.RWMutex`, and why you sharded (lock contention).
- **A clear story arc.** TCP accept loop → per-connection goroutine → protocol parse →
  command dispatch → concurrent-safe store → response encode. You can whiteboard the whole
  data path in an interview.

---

## Architecture (the data path)

```
                 ┌────────────────────────────────────────────────┐
  redis-cli ───► │  TCP listener (Accept loop, 1 goroutine)         │
  clients        │        │ spawns one goroutine per connection     │
                 │        ▼                                         │
                 │  conn handler ──► RESP parser ──► command router │
                 │        ▲                              │          │
                 │        │ RESP encoder ◄───────────────┘          │
                 │        │                              ▼          │
                 │        └────────────── Store (N sharded maps)    │
                 │                         each shard: RWMutex + map │
                 │                         background TTL sweeper    │
                 └────────────────────────────────────────────────┘
```

### Concurrency design (the part interviewers probe)
- **One goroutine per connection.** Go's cheap goroutines make the classic
  "goroutine-per-connection" model practical to tens of thousands of clients.
- **Sharded store (lock striping).** The keyspace is split into N shards, each with its own
  `sync.RWMutex`. Two clients writing different keys usually hit different shards → no
  contention. A single global lock would serialize everything and kill throughput.
- **Background TTL sweeper.** A dedicated goroutine periodically samples keys and evicts
  expired ones (like Redis's active expiration), plus lazy expiry on read.
- **`context` for shutdown.** `SIGINT` cancels a root context; the accept loop stops, live
  connections drain. Graceful shutdown is a senior signal.

---

## Feature roadmap (build in this order)

**Milestone 1 — Core (MVP, ships the story)**
- [x] TCP server + goroutine-per-connection
- [x] RESP2 parser + encoder
- [x] `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`
- [x] Sharded, concurrency-safe store
- [ ] `redis-benchmark` numbers in README

**Milestone 2 — Expiry & types** ✅
- [x] `SET key val EX seconds` / `PX` (+ `NX`/`XX`), `TTL`/`PTTL`, `EXPIRE`/`PEXPIRE`, `PERSIST`
- [x] Active (background sweeper) + lazy (on-read) TTL expiration
- [x] `INCR`/`DECR`/`INCRBY`/`DECRBY`, `APPEND`, `GETSET`, `STRLEN`, `MGET`/`MSET`
- [x] `TYPE`, `KEYS` (glob), `DBSIZE`, `FLUSHDB`
- [x] Hashes: `HSET`/`HGET`/`HDEL`/`HGETALL`/`HKEYS`/`HVALS`/`HLEN`/`HEXISTS`
- [x] Lists: `LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LRANGE`/`LLEN`/`LINDEX`
- [x] Typed value struct (type tag + string/hash/list), `WRONGTYPE` enforcement

**Milestone 3 — Persistence & pub/sub (the flex)**
- [x] AOF (append-only file) persistence + replay on startup (single writer goroutine)
- [x] AOF rewrite / compaction (snapshot → temp file → atomic rename)
- [x] `SUBSCRIBE`/`UNSUBSCRIBE`/`PUBLISH` (channels + goroutine fan-out, non-blocking delivery)
- [ ] Snapshotting (RDB-style) with copy-on-write — *out of scope for v1.0 (AOF covers durability)*

**Milestone 4 — Ops polish** ✅
- [x] `INFO` / basic metrics (uptime, connected clients, total commands — atomics)
- [x] Configurable shard count, sweep interval, AOF path, maxclients
- [x] Dockerfile (multi-stage, static `CGO_ENABLED=0` binary) + graceful shutdown
- [x] Makefile + `make.ps1` (build/test/run/bench) + `.gitignore`

### Intentionally out of scope for v1.0
- **RDB snapshotting / replication / clustering** — AOF is the single durability story.
- **Eviction policies (LRU/LFU) & max-memory** — the store grows unbounded (a dev tool).
- **Transactions (`MULTI`/`EXEC`), Lua scripting, streams, sorted sets, bitmaps.**
- **`RESP3`** — we speak `RESP2` only (pub/sub replies use RESP2 push arrays).
- **Auth / TLS** — bind to localhost / a trusted network.

---

## How to prove concurrency (put these in the README)

```bash
# install redis-tools first (redis-cli + redis-benchmark)
go run ./cmd/server            # starts voltkv on :6380

redis-cli -p 6380 PING         # -> PONG   (real client talks to your server!)
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo      # -> "bar"

# 200 concurrent clients, 100k SET/GET ops — this is your headline number
redis-benchmark -p 6380 -t set,get -c 200 -n 100000 -q
```

README should include a table: **throughput vs shard count (1, 4, 16, 64 shards)** to
visually prove the sharding decision mattered. That single chart says "I understand
concurrency" louder than any sentence.

Also run the built-in race detector as evidence of correctness:
```bash
go test -race ./...
```

---

## Resume bullets (tailor the numbers to what you measure)

- Built a Redis-compatible in-memory data store in Go implementing the RESP2 wire protocol,
  interoperable with `redis-cli` and standard Redis client libraries.
- Designed a lock-striped (sharded) keyspace with per-shard `sync.RWMutex`, sustaining
  **~XXX k ops/sec** under 200 concurrent clients (`redis-benchmark`), a **N×** improvement
  over a single-global-lock baseline.
- Implemented goroutine-per-connection concurrency with `context`-based graceful shutdown,
  active + lazy TTL expiration, and AOF persistence; verified race-free with `go test -race`.

---

## Repo layout

```
voltkv/
  cmd/server/main.go           entrypoint: flags, listener, signal handling
  internal/server/server.go    accept loop, per-conn handler, metrics, AOF wiring, pub/sub delivery
  internal/resp/resp.go        RESP2 reader/writer (arrays, bulk strings, integers, nulls)
  internal/store/store.go      sharded, TTL-aware, typed (string/hash/list) key-value store
  internal/store/helpers.go    errors, int parsing, glob matcher
  internal/store/*_test.go     type/expiry/glob/concurrency + sharding benchmark
  internal/command/command.go  command dispatch table
  internal/command/helpers.go  SET option parsing
  internal/pubsub/pubsub.go    channel-based publish/subscribe broker
  internal/aof/aof.go          append-only-file: single-writer log, replay, rewrite
  internal/aof/aof_test.go     AOF replay + rewrite + pub/sub tests
  Dockerfile  Makefile  make.ps1  .gitignore
```
