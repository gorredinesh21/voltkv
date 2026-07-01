// Package command maps parsed client commands to store operations and writes
// the RESP reply. Adding a new command == adding a case to Execute.
//
// The executor is deliberately thin: it validates arguments, calls into the
// store / pub-sub broker, mirrors write commands to the AOF, and encodes the
// reply. All the concurrency-safety lives in the store and broker.
package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/gorredinesh21/voltkv/internal/aof"
	"github.com/gorredinesh21/voltkv/internal/pubsub"
	"github.com/gorredinesh21/voltkv/internal/resp"
	"github.com/gorredinesh21/voltkv/internal/store"
)

// Stats is the shared counter set the INFO command reports. All fields are read
// and written with sync/atomic by the caller, so INFO sees consistent values
// without a lock. (Defined via an interface so the command package doesn't
// depend on the server package — avoids an import cycle.)
type Stats interface {
	Snapshot() (uptime time.Duration, clients int64, commands int64)
}

// AOFInfo exposes AOF status to the INFO command without a hard dependency.
type AOFInfo interface {
	Enabled() bool
	Size() int64
}

// Deps bundles everything the executor needs. One Deps is shared by all
// connections (its parts are individually concurrency-safe).
type Deps struct {
	Store  *store.Store
	AOF    *aof.Log      // nil when persistence is disabled
	Broker *pubsub.Broker
	Stats  Stats
	AOFI   AOFInfo
}

// ConnState is per-connection state. A connection that has issued SUBSCRIBE
// enters "subscribe mode" and may only run a restricted command set until it
// unsubscribes from everything.
type ConnState struct {
	// Subs maps channel name -> the broker subscription feeding this connection.
	Subs map[string]*pubsub.Subscription
	// Deliver is called by SUBSCRIBE to start a goroutine that forwards messages
	// from a subscription to this client. Set by the server.
	Deliver func(sub *pubsub.Subscription)
}

// InSubscribeMode reports whether the connection currently has subscriptions.
func (c *ConnState) InSubscribeMode() bool { return len(c.Subs) > 0 }

// Execute runs a single command (args[0] is the command name) against the deps
// and writes the reply through w. It never returns store-level errors to the
// caller; protocol/type errors are written as RESP error replies. It returns a
// non-nil error only when the connection should be closed (it currently never
// does — write failures are surfaced by the caller's Flush).
func Execute(d Deps, cs *ConnState, w *resp.Writer, args []string) error {
	if len(args) == 0 {
		return resp.ErrEmptyCommand
	}
	name := strings.ToUpper(args[0])

	// In subscribe mode Redis only allows a handful of commands. Enforce that so
	// a subscribed client can't accidentally run data commands with the wrong
	// reply framing.
	if cs.InSubscribeMode() {
		switch name {
		case "SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT":
			// allowed
		default:
			return w.WriteError("ERR only (UN)SUBSCRIBE / PING / QUIT allowed in subscribe mode")
		}
	}

	switch name {
	// ---- connection ----
	case "PING":
		if len(args) > 1 {
			return w.WriteBulkString(args[1])
		}
		return w.WriteSimpleString("PONG")
	case "ECHO":
		if len(args) != 2 {
			return argErr(w, "echo")
		}
		return w.WriteBulkString(args[1])
	case "COMMAND":
		// redis-cli sends COMMAND DOCS on connect; just acknowledge.
		return w.WriteSimpleString("OK")
	case "QUIT":
		_ = w.WriteSimpleString("OK")
		return resp.ErrEmptyCommand // signal caller to close (handled as EOF-ish)

	// ---- strings ----
	case "SET":
		return cmdSet(d, w, args)
	case "GET":
		return cmdGet(d, w, args)
	case "GETSET":
		return cmdGetSet(d, w, args)
	case "APPEND":
		return cmdAppend(d, w, args)
	case "STRLEN":
		return cmdStrLen(d, w, args)
	case "INCR":
		return cmdIncrBy(d, w, args, 1, false)
	case "DECR":
		return cmdIncrBy(d, w, args, -1, false)
	case "INCRBY":
		return cmdIncrBy(d, w, args, 0, true)
	case "DECRBY":
		return cmdIncrBy(d, w, args, 0, true)
	case "MGET":
		return cmdMGet(d, w, args)
	case "MSET":
		return cmdMSet(d, w, args)

	// ---- generic keys ----
	case "DEL":
		return cmdDel(d, w, args)
	case "EXISTS":
		return cmdExists(d, w, args)
	case "TYPE":
		return cmdType(d, w, args)
	case "EXPIRE":
		return cmdExpire(d, w, args, time.Second)
	case "PEXPIRE":
		return cmdExpire(d, w, args, time.Millisecond)
	case "TTL":
		return cmdTTL(d, w, args, false)
	case "PTTL":
		return cmdTTL(d, w, args, true)
	case "PERSIST":
		return cmdPersist(d, w, args)
	case "KEYS":
		return cmdKeys(d, w, args)
	case "DBSIZE":
		return w.WriteInteger(d.Store.DBSize())
	case "FLUSHDB":
		d.Store.FlushDB()
		appendAOF(d, args)
		return w.WriteSimpleString("OK")

	// ---- hashes ----
	case "HSET":
		return cmdHSet(d, w, args)
	case "HGET":
		return cmdHGet(d, w, args)
	case "HDEL":
		return cmdHDel(d, w, args)
	case "HGETALL":
		return cmdHGetAll(d, w, args)
	case "HKEYS":
		return cmdHCollection(d, w, args, d.Store.HKeys, "hkeys")
	case "HVALS":
		return cmdHCollection(d, w, args, d.Store.HVals, "hvals")
	case "HLEN":
		return cmdHLen(d, w, args)
	case "HEXISTS":
		return cmdHExists(d, w, args)

	// ---- lists ----
	case "LPUSH":
		return cmdPush(d, w, args, true)
	case "RPUSH":
		return cmdPush(d, w, args, false)
	case "LPOP":
		return cmdPop(d, w, args, true)
	case "RPOP":
		return cmdPop(d, w, args, false)
	case "LLEN":
		return cmdLLen(d, w, args)
	case "LINDEX":
		return cmdLIndex(d, w, args)
	case "LRANGE":
		return cmdLRange(d, w, args)

	// ---- pub/sub ----
	case "SUBSCRIBE":
		return cmdSubscribe(d, cs, w, args)
	case "UNSUBSCRIBE":
		return cmdUnsubscribe(d, cs, w, args)
	case "PUBLISH":
		return cmdPublish(d, w, args)

	// ---- server ----
	case "INFO":
		return cmdInfo(d, w)

	default:
		return w.WriteError("ERR unknown command '" + args[0] + "'")
	}
}

// appendAOF mirrors a write command to the AOF if persistence is enabled.
// It is a no-op when d.AOF is nil (Append itself guards nil too).
func appendAOF(d Deps, args []string) {
	if d.AOF != nil {
		d.AOF.Append(args...)
	}
}

func argErr(w *resp.Writer, cmd string) error {
	return w.WriteError("ERR wrong number of arguments for '" + cmd + "'")
}

// ---- string command handlers --------------------------------------------------

func cmdSet(d Deps, w *resp.Writer, args []string) error {
	// SET key value [EX seconds | PX ms] [NX | XX]
	if len(args) < 3 {
		return argErr(w, "set")
	}
	ttl, nx, xx, err := parseSetOpts(args[3:])
	if err != nil {
		return w.WriteError(err.Error())
	}
	switch {
	case nx:
		if d.Store.SetNX(args[1], args[2], ttl) {
			appendAOF(d, args)
			return w.WriteSimpleString("OK")
		}
		return w.WriteNull() // condition not met
	case xx:
		if d.Store.SetXX(args[1], args[2], ttl) {
			appendAOF(d, args)
			return w.WriteSimpleString("OK")
		}
		return w.WriteNull()
	default:
		d.Store.Set(args[1], args[2], ttl)
		appendAOF(d, args)
		return w.WriteSimpleString("OK")
	}
}

func cmdGet(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "get")
	}
	v, ok, err := d.Store.Get(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulkString(v)
}

func cmdGetSet(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "getset")
	}
	old, existed, err := d.Store.GetSet(args[1], args[2])
	if err != nil {
		return w.WriteError(err.Error())
	}
	// GetSet is a write: persist it as a plain SET.
	appendAOF(d, []string{"SET", args[1], args[2]})
	if !existed {
		return w.WriteNull()
	}
	return w.WriteBulkString(old)
}

func cmdAppend(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "append")
	}
	n, err := d.Store.Append(args[1], args[2])
	if err != nil {
		return w.WriteError(err.Error())
	}
	appendAOF(d, args)
	return w.WriteInteger(n)
}

func cmdStrLen(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "strlen")
	}
	n, err := d.Store.StrLen(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteInteger(n)
}

// cmdIncrBy handles INCR/DECR (fixed delta) and INCRBY/DECRBY (arg delta).
func cmdIncrBy(d Deps, w *resp.Writer, args []string, fixed int64, byArg bool) error {
	var delta int64
	name := strings.ToUpper(args[0])
	if byArg {
		if len(args) != 3 {
			return argErr(w, strings.ToLower(name))
		}
		n, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return w.WriteError("ERR value is not an integer or out of range")
		}
		delta = n
		if name == "DECRBY" {
			delta = -delta
		}
	} else {
		if len(args) != 2 {
			return argErr(w, strings.ToLower(name))
		}
		delta = fixed
	}
	v, err := d.Store.IncrBy(args[1], delta)
	if err != nil {
		return w.WriteError(err.Error())
	}
	// Persist as a SET of the resulting value so replay is deterministic even if
	// commands are replayed against a non-empty store.
	appendAOF(d, []string{"SET", args[1], strconv.FormatInt(v, 10)})
	return w.WriteInteger64(v)
}

func cmdMGet(d Deps, w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return argErr(w, "mget")
	}
	keys := args[1:]
	vals := make([]string, len(keys))
	ok := make([]bool, len(keys))
	for i, k := range keys {
		v, found, err := d.Store.Get(k)
		if err != nil {
			// Wrong type: Redis returns null for that element, not an error.
			continue
		}
		vals[i], ok[i] = v, found
	}
	return w.WriteNullableStringArray(vals, ok)
}

func cmdMSet(d Deps, w *resp.Writer, args []string) error {
	// MSET key val [key val ...]
	if len(args) < 3 || len(args)%2 != 1 {
		return argErr(w, "mset")
	}
	for i := 1; i+1 < len(args); i += 2 {
		d.Store.Set(args[i], args[i+1], 0)
		appendAOF(d, []string{"SET", args[i], args[i+1]})
	}
	return w.WriteSimpleString("OK")
}

// ---- generic key handlers ------------------------------------------------------

func cmdDel(d Deps, w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return argErr(w, "del")
	}
	removed := 0
	for _, k := range args[1:] {
		if d.Store.Del(k) {
			removed++
		}
	}
	if removed > 0 {
		appendAOF(d, args)
	}
	return w.WriteInteger(removed)
}

func cmdExists(d Deps, w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return argErr(w, "exists")
	}
	count := 0
	for _, k := range args[1:] {
		if d.Store.Exists(k) {
			count++
		}
	}
	return w.WriteInteger(count)
}

func cmdType(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "type")
	}
	return w.WriteSimpleString(d.Store.Type(args[1]).String())
}

func cmdExpire(d Deps, w *resp.Writer, args []string, unit time.Duration) error {
	if len(args) != 3 {
		return argErr(w, strings.ToLower(args[0]))
	}
	amount, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	if d.Store.Expire(args[1], time.Duration(amount)*unit) {
		appendAOF(d, args)
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdTTL(d Deps, w *resp.Writer, args []string, milli bool) error {
	if len(args) != 2 {
		return argErr(w, strings.ToLower(args[0]))
	}
	remaining, found, hasTTL := d.Store.TTL(args[1])
	if !found {
		return w.WriteInteger(-2) // no such key
	}
	if !hasTTL {
		return w.WriteInteger(-1) // key exists but has no TTL
	}
	if milli {
		return w.WriteInteger(int(remaining / time.Millisecond))
	}
	return w.WriteInteger(int(remaining / time.Second))
}

func cmdPersist(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "persist")
	}
	if d.Store.Persist(args[1]) {
		appendAOF(d, args)
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdKeys(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "keys")
	}
	return w.WriteStringArray(d.Store.Keys(args[1]))
}

// ---- hash handlers -------------------------------------------------------------

func cmdHSet(d Deps, w *resp.Writer, args []string) error {
	// HSET key field value [field value ...]
	if len(args) < 4 || len(args)%2 != 0 {
		return argErr(w, "hset")
	}
	pairs := make([][2]string, 0, (len(args)-2)/2)
	for i := 2; i+1 < len(args); i += 2 {
		pairs = append(pairs, [2]string{args[i], args[i+1]})
	}
	added, err := d.Store.HSet(args[1], pairs)
	if err != nil {
		return w.WriteError(err.Error())
	}
	appendAOF(d, args)
	return w.WriteInteger(added)
}

func cmdHGet(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "hget")
	}
	v, ok, err := d.Store.HGet(args[1], args[2])
	if err != nil {
		return w.WriteError(err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulkString(v)
}

func cmdHDel(d Deps, w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return argErr(w, "hdel")
	}
	n, err := d.Store.HDel(args[1], args[2:])
	if err != nil {
		return w.WriteError(err.Error())
	}
	if n > 0 {
		appendAOF(d, args)
	}
	return w.WriteInteger(n)
}

func cmdHGetAll(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "hgetall")
	}
	pairs, err := d.Store.HGetAll(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteStringArray(pairs)
}

// cmdHCollection handles HKEYS and HVALS which share the same shape.
func cmdHCollection(d Deps, w *resp.Writer, args []string, fn func(string) ([]string, error), name string) error {
	if len(args) != 2 {
		return argErr(w, name)
	}
	items, err := fn(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteStringArray(items)
}

func cmdHLen(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "hlen")
	}
	n, err := d.Store.HLen(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteInteger(n)
}

func cmdHExists(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "hexists")
	}
	ex, err := d.Store.HExists(args[1], args[2])
	if err != nil {
		return w.WriteError(err.Error())
	}
	if ex {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

// ---- list handlers -------------------------------------------------------------

func cmdPush(d Deps, w *resp.Writer, args []string, left bool) error {
	if len(args) < 3 {
		return argErr(w, strings.ToLower(args[0]))
	}
	var (
		n   int
		err error
	)
	if left {
		n, err = d.Store.LPush(args[1], args[2:])
	} else {
		n, err = d.Store.RPush(args[1], args[2:])
	}
	if err != nil {
		return w.WriteError(err.Error())
	}
	appendAOF(d, args)
	return w.WriteInteger(n)
}

func cmdPop(d Deps, w *resp.Writer, args []string, left bool) error {
	if len(args) != 2 {
		return argErr(w, strings.ToLower(args[0]))
	}
	var (
		v   string
		ok  bool
		err error
	)
	if left {
		v, ok, err = d.Store.LPop(args[1])
	} else {
		v, ok, err = d.Store.RPop(args[1])
	}
	if err != nil {
		return w.WriteError(err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	appendAOF(d, args)
	return w.WriteBulkString(v)
}

func cmdLLen(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return argErr(w, "llen")
	}
	n, err := d.Store.LLen(args[1])
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteInteger(n)
}

func cmdLIndex(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "lindex")
	}
	idx, err := strconv.Atoi(args[2])
	if err != nil {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	v, ok, serr := d.Store.LIndex(args[1], idx)
	if serr != nil {
		return w.WriteError(serr.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulkString(v)
}

func cmdLRange(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 4 {
		return argErr(w, "lrange")
	}
	start, e1 := strconv.Atoi(args[2])
	stop, e2 := strconv.Atoi(args[3])
	if e1 != nil || e2 != nil {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	items, err := d.Store.LRange(args[1], start, stop)
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteStringArray(items)
}

// ---- pub/sub handlers ----------------------------------------------------------

func cmdSubscribe(d Deps, cs *ConnState, w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return argErr(w, "subscribe")
	}
	for _, ch := range args[1:] {
		if _, already := cs.Subs[ch]; already {
			continue
		}
		sub := d.Broker.Subscribe(ch)
		cs.Subs[ch] = sub
		// Start forwarding messages for this subscription to the client.
		if cs.Deliver != nil {
			cs.Deliver(sub)
		}
		// Reply: ["subscribe", channel, current-subscription-count].
		if err := writeSubscribeReply(w, "subscribe", ch, len(cs.Subs)); err != nil {
			return err
		}
	}
	return nil
}

func cmdUnsubscribe(d Deps, cs *ConnState, w *resp.Writer, args []string) error {
	// UNSUBSCRIBE with no channels means "all".
	channels := args[1:]
	if len(channels) == 0 {
		for ch := range cs.Subs {
			channels = append(channels, ch)
		}
		// If we weren't subscribed to anything, still send one reply (Redis does).
		if len(channels) == 0 {
			return writeSubscribeReplyNull(w, "unsubscribe", 0)
		}
	}
	for _, ch := range channels {
		if sub, ok := cs.Subs[ch]; ok {
			d.Broker.Unsubscribe(sub)
			delete(cs.Subs, ch)
		}
		if err := writeSubscribeReply(w, "unsubscribe", ch, len(cs.Subs)); err != nil {
			return err
		}
	}
	return nil
}

func cmdPublish(d Deps, w *resp.Writer, args []string) error {
	if len(args) != 3 {
		return argErr(w, "publish")
	}
	n := d.Broker.Publish(args[1], args[2])
	return w.WriteInteger(n)
}

// writeSubscribeReply emits the 3-element push array Redis uses to confirm a
// (un)subscribe: [kind, channel, count].
func writeSubscribeReply(w *resp.Writer, kind, channel string, count int) error {
	if err := w.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := w.WriteBulkString(kind); err != nil {
		return err
	}
	if err := w.WriteBulkString(channel); err != nil {
		return err
	}
	return w.WriteInteger(count)
}

// writeSubscribeReplyNull is the unsubscribe-from-nothing reply: [kind, nil, 0].
func writeSubscribeReplyNull(w *resp.Writer, kind string, count int) error {
	if err := w.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := w.WriteBulkString(kind); err != nil {
		return err
	}
	if err := w.WriteNull(); err != nil {
		return err
	}
	return w.WriteInteger(count)
}

// ---- server handlers -----------------------------------------------------------

func cmdInfo(d Deps, w *resp.Writer) error {
	var b strings.Builder
	b.WriteString("# Server\r\n")
	if d.Stats != nil {
		uptime, clients, commands := d.Stats.Snapshot()
		b.WriteString("uptime_seconds:" + strconv.FormatInt(int64(uptime/time.Second), 10) + "\r\n")
		b.WriteString("connected_clients:" + strconv.FormatInt(clients, 10) + "\r\n")
		b.WriteString("total_commands_processed:" + strconv.FormatInt(commands, 10) + "\r\n")
	}
	b.WriteString("# Keyspace\r\n")
	b.WriteString("db_keys:" + strconv.Itoa(d.Store.DBSize()) + "\r\n")
	b.WriteString("shards:" + strconv.Itoa(d.Store.NumShards()) + "\r\n")
	b.WriteString("# Persistence\r\n")
	if d.AOFI != nil && d.AOFI.Enabled() {
		b.WriteString("aof_enabled:1\r\n")
		b.WriteString("aof_size_bytes:" + strconv.FormatInt(d.AOFI.Size(), 10) + "\r\n")
	} else {
		b.WriteString("aof_enabled:0\r\n")
	}
	return w.WriteBulkString(b.String())
}
