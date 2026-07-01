// Command voltkv starts a Redis-compatible in-memory store.
//
//	go run ./cmd/server -addr :6380 -shards 16 -aof voltkv.aof
//	redis-cli -p 6380 PING
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorredinesh21/voltkv/internal/server"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	shards := flag.Int("shards", 16, "number of keyspace shards (power of two)")
	sweep := flag.Duration("sweep", time.Second, "TTL sweep interval")
	aofPath := flag.String("aof", "", "append-only file path (empty = persistence disabled)")
	maxClients := flag.Int("maxclients", 0, "maximum concurrent clients (0 = unlimited)")
	flag.Parse()

	// Cancel the root context on Ctrl-C / SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := server.New(server.Config{
		Addr:       *addr,
		Shards:     *shards,
		SweepEvery: *sweep,
		AOFPath:    *aofPath,
		MaxClients: *maxClients,
	})
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("voltkv: %v", err)
	}
}
