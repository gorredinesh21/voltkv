// Package server wires the TCP listener to the RESP parser, command dispatcher,
// store, pub/sub broker, and AOF persistence. It uses the classic
// goroutine-per-connection model and shuts down gracefully when its context is
// cancelled.
package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorredinesh21/voltkv/internal/aof"
	"github.com/gorredinesh21/voltkv/internal/command"
	"github.com/gorredinesh21/voltkv/internal/pubsub"
	"github.com/gorredinesh21/voltkv/internal/resp"
	"github.com/gorredinesh21/voltkv/internal/store"
)

type Config struct {
	Addr       string        // e.g. ":6380"
	Shards     int           // number of store shards (power of two recommended)
	SweepEvery time.Duration // how often the background TTL sweeper runs
	AOFPath    string        // append-only file path; empty disables persistence
	MaxClients int           // 0 = unlimited
}

type Server struct {
	cfg    Config
	store  *store.Store
	broker *pubsub.Broker
	aof    *aof.Log // nil when persistence disabled
	wg     sync.WaitGroup

	// Metrics tracked with atomics so INFO reads them without a lock.
	startedAt     time.Time
	clients       atomic.Int64 // currently connected clients
	totalCommands atomic.Int64 // commands processed since start
}

func New(cfg Config) *Server {
	if cfg.Shards <= 0 {
		cfg.Shards = 16
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = time.Second
	}
	return &Server{
		cfg:       cfg,
		store:     store.New(cfg.Shards),
		broker:    pubsub.New(),
		startedAt: time.Now(),
	}
}

// Snapshot implements command.Stats — returns a consistent-enough view of the
// server counters for INFO.
func (s *Server) Snapshot() (uptime time.Duration, clients int64, commands int64) {
	return time.Since(s.startedAt), s.clients.Load(), s.totalCommands.Load()
}

// Enabled / Size implement command.AOFInfo for the INFO command.
func (s *Server) Enabled() bool { return s.aof != nil }
func (s *Server) Size() int64   { return s.aof.Size() }

// deps builds the shared command dependencies for this server.
func (s *Server) deps() command.Deps {
	return command.Deps{
		Store:  s.store,
		AOF:    s.aof,
		Broker: s.broker,
		Stats:  s,
		AOFI:   s,
	}
}

// Run blocks, accepting connections until ctx is cancelled. On cancellation it
// stops accepting and waits for in-flight connections to finish.
func (s *Server) Run(ctx context.Context) error {
	// --- AOF: open the log and replay existing history into the store BEFORE we
	// start serving, so clients see the recovered state. ---
	if s.cfg.AOFPath != "" {
		// Replay first (reads the file) using a temporary "silent" executor that
		// applies commands to the store without logging them again.
		if err := s.replayAOF(); err != nil {
			return err
		}
		lg, err := aof.Open(s.cfg.AOFPath)
		if err != nil {
			return err
		}
		s.aof = lg
		defer s.aof.Close()
		log.Printf("voltkv: AOF enabled at %s (%d keys after replay)", s.cfg.AOFPath, s.store.DBSize())
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	log.Printf("voltkv listening on %s (%d shards)", s.cfg.Addr, s.cfg.Shards)

	// Background TTL sweeper (active expiration).
	s.wg.Add(1)
	go s.sweepLoop(ctx)

	// Close the listener when the context is cancelled so Accept() unblocks.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			log.Printf("accept error: %v", err)
			continue
		}
		// Enforce maxclients: reject politely if we're at capacity.
		if s.cfg.MaxClients > 0 && s.clients.Load() >= int64(s.cfg.MaxClients) {
			w := resp.NewWriter(conn)
			_ = w.WriteError("ERR max number of clients reached")
			_ = w.Flush()
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go s.handleConn(ctx, conn)
	}

	log.Print("voltkv: draining connections...")
	s.wg.Wait()
	log.Print("voltkv: stopped")
	return nil
}

// replayAOF rebuilds the store from the AOF file. It runs commands through the
// normal executor but with a Deps whose AOF is nil, so replayed writes are not
// re-appended. It also uses a throwaway ConnState (pub/sub is meaningless here).
func (s *Server) replayAOF() error {
	deps := command.Deps{Store: s.store, Broker: s.broker} // AOF nil => no re-logging
	sink := resp.NewWriter(io.Discard)                     // replies are discarded on replay
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}
	return aof.Replay(s.cfg.AOFPath, func(args []string) error {
		_ = command.Execute(deps, cs, sink, args)
		return sink.Flush()
	})
}

func (s *Server) sweepLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.SweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.store.SweepExpired()
		}
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	s.clients.Add(1)
	defer s.clients.Add(-1)

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	// Per-connection pub/sub state. Delivery of published messages happens on
	// separate goroutines (one per subscription) that write to this same
	// connection. To keep those writes from interleaving with command replies
	// (both use the shared writer), we serialize all writes through writeMu.
	var writeMu sync.Mutex
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}

	// A WaitGroup so we don't close the connection until all delivery goroutines
	// for this connection have stopped.
	var deliverWG sync.WaitGroup
	cs.Deliver = func(sub *pubsub.Subscription) {
		deliverWG.Add(1)
		go func() {
			defer deliverWG.Done()
			// Range ends when the broker closes sub.C (on unsubscribe/disconnect).
			for msg := range sub.C {
				writeMu.Lock()
				// Deliver as a 3-element push: ["message", channel, payload].
				_ = writer.WriteArrayHeader(3)
				_ = writer.WriteBulkString("message")
				_ = writer.WriteBulkString(msg.Channel)
				_ = writer.WriteBulkString(msg.Payload)
				_ = writer.Flush()
				writeMu.Unlock()
			}
		}()
	}

	// On disconnect, drop all subscriptions so their goroutines exit and the
	// broker stops holding references to this connection.
	defer func() {
		for ch, sub := range cs.Subs {
			s.broker.Unsubscribe(sub)
			delete(cs.Subs, ch)
		}
		deliverWG.Wait()
	}()

	deps := s.deps()

	for {
		// Cheap shutdown check between commands.
		if ctx.Err() != nil {
			return
		}
		args, err := reader.ReadCommand()
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		s.totalCommands.Add(1)

		writeMu.Lock()
		execErr := command.Execute(deps, cs, writer, args)
		flushErr := writer.Flush()
		writeMu.Unlock()

		if execErr != nil || flushErr != nil {
			return
		}
	}
}
