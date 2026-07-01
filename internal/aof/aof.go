// Package aof implements append-only-file persistence, the same durability model
// Redis calls AOF. Every write command the server executes is serialized to RESP
// and appended to a file. On startup the file is replayed to rebuild the exact
// dataset. A rewrite step compacts the file down to the minimum set of commands
// that recreate the current state.
//
// Concurrency design: a SINGLE writer goroutine owns the file. Connection
// goroutines never touch the file directly — they hand serialized commands to
// the writer over a buffered channel. This makes appends naturally ordered and
// race-free without any per-connection locking, and it keeps disk I/O off the
// command hot path (fire-and-forget from the caller's perspective).
package aof

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/gorredinesh21/voltkv/internal/resp"
	"github.com/gorredinesh21/voltkv/internal/store"
)

// Log is an append-only command log backed by a file and a single writer
// goroutine. The zero value is not usable; call Open.
type Log struct {
	path string

	ch   chan []string  // commands queued for the writer goroutine
	done chan struct{}  // closed when the writer goroutine has exited
	wg   sync.WaitGroup // tracks the writer goroutine

	mu   sync.Mutex // guards file swaps during rewrite and Size()
	f    *os.File
	w    *bufio.Writer
}

// Open opens (creating if needed) the AOF at path and starts the writer
// goroutine. Existing content is preserved; new appends go to the end.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		path: path,
		ch:   make(chan []string, 1024),
		done: make(chan struct{}),
		f:    f,
		w:    bufio.NewWriter(f),
	}
	l.wg.Add(1)
	go l.writeLoop()
	return l, nil
}

// writeLoop is the single writer. It drains the channel, encodes each command as
// a RESP array, and flushes once the channel is momentarily empty (batching for
// throughput while still being durable-ish for a demo).
func (l *Log) writeLoop() {
	defer l.wg.Done()
	defer close(l.done)
	for cmd := range l.ch {
		l.mu.Lock()
		writeCommand(l.w, cmd)
		// If nothing else is queued, flush now so data reaches the OS promptly.
		if len(l.ch) == 0 {
			_ = l.w.Flush()
		}
		l.mu.Unlock()
	}
	// Final flush on shutdown.
	l.mu.Lock()
	_ = l.w.Flush()
	l.mu.Unlock()
}

// Append queues a write command for durable logging. It is safe to call from any
// number of connection goroutines. The send is buffered; if the buffer is full
// this blocks briefly until the writer catches up (back-pressure, not data loss).
func (l *Log) Append(args ...string) {
	if l == nil {
		return // AOF disabled: no-op so callers don't need nil checks everywhere
	}
	l.ch <- args
}

// Close stops the writer goroutine, flushes, and closes the file.
func (l *Log) Close() error {
	if l == nil {
		return nil
	}
	close(l.ch)
	<-l.done
	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// Size returns the current on-disk size of the AOF in bytes (best-effort).
func (l *Log) Size() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.w.Flush()
	fi, err := l.f.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// writeCommand serializes one command as a RESP array of bulk strings — exactly
// the format a client would send — so replay can reuse the normal command path.
func writeCommand(w *bufio.Writer, args []string) {
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a)
	}
}

// Replay reads every command from path and calls apply for each one, in order.
// apply is typically the server's command executor pointed at a fresh store.
// A missing file is not an error (nothing to replay).
func Replay(path string, apply func(args []string) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := resp.NewReader(f)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			// EOF (or a torn trailing write) simply ends replay.
			return nil
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return err
		}
	}
}

// Rewrite compacts the AOF: it writes a minimal set of commands that recreates
// snap (the current dataset) to a temp file, then atomically replaces the live
// file. The writer goroutine keeps running; we swap the *os.File it writes to
// under l.mu so no appends are lost or interleaved with the swap.
func (l *Log) Rewrite(snap []store.Snapshot) error {
	if l == nil {
		return nil
	}
	tmp := l.path + ".rewrite"
	tf, err := os.Create(tmp)
	if err != nil {
		return err
	}
	tw := bufio.NewWriter(tf)
	for _, s := range snap {
		for _, cmd := range snapshotCommands(s) {
			writeCommand(tw, cmd)
		}
	}
	if err := tw.Flush(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}

	// Swap the live file under the lock so the writer goroutine can't append
	// mid-rename. We flush the old buffer, close the old file, rename the temp
	// over the real path, then reopen for appends.
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.w.Flush()
	_ = l.f.Close()
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	nf, err := os.OpenFile(l.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	l.f = nf
	l.w = bufio.NewWriter(nf)
	return nil
}

// snapshotCommands turns one key's snapshot into the command(s) that recreate it,
// including a trailing PEXPIREAT-style EXPIRE if the key has a TTL.
func snapshotCommands(s store.Snapshot) [][]string {
	var cmds [][]string
	switch s.Kind {
	case store.KindString:
		cmds = append(cmds, []string{"SET", s.Key, s.Str})
	case store.KindHash:
		if len(s.Hash) > 0 {
			args := []string{"HSET", s.Key}
			for f, v := range s.Hash {
				args = append(args, f, v)
			}
			cmds = append(cmds, args)
		}
	case store.KindList:
		if len(s.List) > 0 {
			args := append([]string{"RPUSH", s.Key}, s.List...)
			cmds = append(cmds, args)
		}
	}
	// Preserve TTL by emitting a PEXPIRE with the remaining milliseconds.
	if !s.ExpiresAt.IsZero() && len(cmds) > 0 {
		ms := msUntil(s.ExpiresAt)
		if ms > 0 {
			cmds = append(cmds, []string{"PEXPIRE", s.Key, strconv.FormatInt(ms, 10)})
		}
	}
	return cmds
}
