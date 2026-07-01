package aof_test

import (
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorredinesh21/voltkv/internal/aof"
	"github.com/gorredinesh21/voltkv/internal/command"
	"github.com/gorredinesh21/voltkv/internal/pubsub"
	"github.com/gorredinesh21/voltkv/internal/resp"
	"github.com/gorredinesh21/voltkv/internal/store"
)

// run executes a command against st, logging it to lg (nil = no logging).
func run(t *testing.T, st *store.Store, lg *aof.Log, args ...string) {
	t.Helper()
	d := command.Deps{Store: st, AOF: lg, Broker: pubsub.New()}
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}
	w := resp.NewWriter(io.Discard)
	if err := command.Execute(d, cs, w, args); err != nil {
		t.Fatalf("execute %v: %v", args, err)
	}
	_ = w.Flush()
}

// replay rebuilds a fresh store by replaying the AOF at path.
func replay(t *testing.T, path string) *store.Store {
	t.Helper()
	st := store.New(8)
	d := command.Deps{Store: st, Broker: pubsub.New()} // AOF nil => no re-logging
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}
	w := resp.NewWriter(io.Discard)
	err := aof.Replay(path, func(args []string) error {
		_ = command.Execute(d, cs, w, args)
		return w.Flush()
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return st
}

// TestAOFReplay writes a mix of string/hash/list/expiry commands, "restarts" by
// creating a fresh store and replaying the AOF, then asserts the state matches.
func TestAOFReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	lg, err := aof.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	run(t, store.New(8), lg, "PING") // non-write: should not be logged, harmless
	// Use one live store for writing so we exercise the real command paths.
	src := store.New(8)
	d := command.Deps{Store: src, AOF: lg, Broker: pubsub.New()}
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}
	w := resp.NewWriter(io.Discard)
	exec := func(args ...string) {
		if err := command.Execute(d, cs, w, args); err != nil {
			t.Fatalf("exec %v: %v", args, err)
		}
		_ = w.Flush()
	}

	exec("SET", "greeting", "hello")
	exec("INCR", "counter")
	exec("INCR", "counter")
	exec("APPEND", "greeting", " world")
	exec("HSET", "user", "name", "dinesh", "role", "eng")
	exec("RPUSH", "queue", "a", "b", "c")
	exec("LPUSH", "queue", "z")
	exec("SET", "temp", "soon", "PX", "500")
	exec("DEL", "counter")

	if err := lg.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": fresh store from the AOF.
	got := replay(t, path)

	if v, ok, _ := got.Get("greeting"); !ok || v != "hello world" {
		t.Fatalf("greeting after replay: got %q,%v", v, ok)
	}
	if got.Type("counter") != store.KindNone {
		t.Fatal("counter was DELeted; should be absent after replay")
	}
	if v, ok, _ := got.HGet("user", "name"); !ok || v != "dinesh" {
		t.Fatalf("user.name after replay: got %q,%v", v, ok)
	}
	items, _ := got.LRange("queue", 0, -1)
	if !sameList(items, []string{"z", "a", "b", "c"}) {
		t.Fatalf("queue after replay: got %v", items)
	}
	// The temp key had a short TTL preserved via the logged SET ... PX.
	if _, _, hasTTL := got.TTL("temp"); !hasTTL {
		t.Fatal("temp should retain a TTL after replay")
	}
}

// TestAOFRewrite compacts the AOF and verifies replay of the rewritten file
// reproduces the same dataset with fewer commands.
func TestAOFRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rewrite.aof")
	lg, err := aof.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	src := store.New(8)
	d := command.Deps{Store: src, AOF: lg, Broker: pubsub.New()}
	cs := &command.ConnState{Subs: map[string]*pubsub.Subscription{}}
	w := resp.NewWriter(io.Discard)
	exec := func(args ...string) {
		_ = command.Execute(d, cs, w, args)
		_ = w.Flush()
	}

	// Lots of churn that a rewrite should collapse.
	for i := 0; i < 50; i++ {
		exec("INCR", "n")
	}
	exec("HSET", "h", "f", "v")
	exec("RPUSH", "l", "x", "y")
	exec("SET", "s", "final")

	// Rewrite from the current snapshot.
	if err := lg.Rewrite(src.SnapshotAll()); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := lg.Close(); err != nil {
		t.Fatal(err)
	}

	got := replay(t, path)
	if v, _, _ := got.Get("n"); v != "50" {
		t.Fatalf("n after rewrite+replay: got %q want 50", v)
	}
	if v, ok, _ := got.HGet("h", "f"); !ok || v != "v" {
		t.Fatalf("h.f: got %q,%v", v, ok)
	}
	items, _ := got.LRange("l", 0, -1)
	if !sameList(items, []string{"x", "y"}) {
		t.Fatalf("l after rewrite: got %v", items)
	}
	if v, _, _ := got.Get("s"); v != "final" {
		t.Fatalf("s: got %q", v)
	}
}

func TestPubSub(t *testing.T) {
	b := pubsub.New()
	sub := b.Subscribe("news")
	if n := b.Publish("news", "hello"); n != 1 {
		t.Fatalf("publish delivered %d want 1", n)
	}
	select {
	case msg := <-sub.C:
		if msg.Channel != "news" || msg.Payload != "hello" {
			t.Fatalf("bad message: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
	// Publishing to a channel with no subscribers delivers to 0.
	if n := b.Publish("empty", "x"); n != 0 {
		t.Fatalf("publish to empty: got %d want 0", n)
	}
	b.Unsubscribe(sub)
	// After unsubscribe the channel is closed; range would end. Publishing is 0.
	if n := b.Publish("news", "gone"); n != 0 {
		t.Fatalf("publish after unsub: got %d want 0", n)
	}
}

func sameList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
