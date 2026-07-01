// Package store implements a sharded, concurrency-safe, TTL-aware key-value store.
//
// The keyspace is split into N shards, each guarded by its own RWMutex. This is
// "lock striping": two goroutines operating on keys that hash to different shards
// never contend on the same lock, so throughput scales with core count instead of
// serializing behind one global mutex.
//
// Values are typed. A single key holds exactly one of: a string, a hash
// (map[string]string), or a list ([]string) — just like Redis. Every value is
// wrapped in an `entry` that carries a type tag plus an optional expiry. All
// mutations happen under the owning shard's lock, so the maps/slices inside an
// entry are never touched by two goroutines at once.
package store

import (
	"hash/fnv"
	"sync"
	"time"
)

// Kind is the type tag for a stored value. Redis keys are dynamically typed but
// a given key only ever holds one kind at a time; operating on the wrong kind is
// a WRONGTYPE error, which the command layer surfaces.
type Kind uint8

const (
	KindNone   Kind = iota // key does not exist
	KindString             // a plain string value
	KindHash               // a field->value map
	KindList               // an ordered list of strings
)

// String makes Kind print nicely for the TYPE command.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindHash:
		return "hash"
	case KindList:
		return "list"
	default:
		return "none"
	}
}

// entry is a single stored value with an optional expiry. Only one of the
// payload fields is meaningful, selected by kind.
type entry struct {
	kind      Kind
	str       string            // valid when kind == KindString
	hash      map[string]string // valid when kind == KindHash
	list      []string          // valid when kind == KindList
	expiresAt time.Time         // zero value == never expires
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// shard is one slice of the keyspace with its own lock.
type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a sharded key-value store. Safe for concurrent use by many goroutines.
type Store struct {
	shards []*shard
	mask   uint32 // len(shards)-1; requires shard count to be a power of two
}

// New creates a Store with shardCount shards. shardCount is rounded up to the
// next power of two so we can index shards with a cheap bit-mask instead of a
// modulo. More shards == less lock contention (up to a point).
func New(shardCount int) *Store {
	n := nextPow2(shardCount)
	s := &Store{shards: make([]*shard, n), mask: uint32(n - 1)}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]entry)}
	}
	return s
}

// NumShards reports how many shards back the store (used by INFO).
func (s *Store) NumShards() int { return len(s.shards) }

// shardFor returns the shard responsible for key.
func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()&s.mask]
}

// getLive returns the live (non-expired) entry for key while the caller already
// holds sh's lock. It reports found=false for missing or expired keys. It does
// NOT delete expired keys (the caller may only hold a read lock); expiry cleanup
// happens on the write paths and in the sweeper.
func getLive(sh *shard, key string, now time.Time) (entry, bool) {
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

// ---- String commands ---------------------------------------------------------

// Set stores a string value under key. If ttl > 0 the key expires after ttl.
// It overwrites any existing value of any type (matching Redis semantics).
func (s *Store) Set(key, value string, ttl time.Duration) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := entry{kind: KindString, str: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	sh.data[key] = e
	sh.mu.Unlock()
}

// SetNX stores value only if key does not already exist (and is not expired).
// Returns true if it stored the value.
func (s *Store) SetNX(key, value string, ttl time.Duration) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := getLive(sh, key, now); ok {
		return false
	}
	e := entry{kind: KindString, str: value}
	if ttl > 0 {
		e.expiresAt = now.Add(ttl)
	}
	sh.data[key] = e
	return true
}

// SetXX stores value only if key already exists. Returns true if it stored.
func (s *Store) SetXX(key, value string, ttl time.Duration) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := getLive(sh, key, now); !ok {
		return false
	}
	e := entry{kind: KindString, str: value}
	if ttl > 0 {
		e.expiresAt = now.Add(ttl)
	}
	sh.data[key] = e
	return true
}

// Get returns the string value for key. ok is false if the key is missing,
// expired, or holds a non-string type. Expired keys are lazily deleted on read
// (like Redis's passive expiration). err is set only for a wrong-type mismatch.
func (s *Store) Get(key string) (value string, ok bool, err error) {
	sh := s.shardFor(key)
	now := time.Now()

	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()
	if !found {
		return "", false, nil
	}
	if e.expired(now) {
		s.lazyDelete(sh, key, now)
		return "", false, nil
	}
	if e.kind != KindString {
		return "", false, ErrWrongType
	}
	return e.str, true, nil
}

// lazyDelete removes an expired key under the write lock, re-checking that it is
// still present and still expired (another goroutine may have refreshed it).
func (s *Store) lazyDelete(sh *shard, key string, now time.Time) {
	sh.mu.Lock()
	if cur, still := sh.data[key]; still && cur.expired(now) {
		delete(sh.data, key)
	}
	sh.mu.Unlock()
}

// Append appends suffix to the string at key (creating it if absent) and returns
// the new length. Errors if key holds a non-string type.
func (s *Store) Append(key, suffix string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		e = entry{kind: KindString}
	} else if e.kind != KindString {
		return 0, ErrWrongType
	}
	e.str += suffix
	sh.data[key] = e
	return len(e.str), nil
}

// GetSet atomically sets key to value and returns the old string value.
func (s *Store) GetSet(key, value string) (old string, existed bool, err error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := getLive(sh, key, now); ok {
		if e.kind != KindString {
			return "", false, ErrWrongType
		}
		old, existed = e.str, true
	}
	// GetSet clears any prior TTL (matches Redis: it's a plain SET).
	sh.data[key] = entry{kind: KindString, str: value}
	return old, existed, nil
}

// StrLen returns the length of the string at key (0 if missing).
func (s *Store) StrLen(key string) (int, error) {
	v, ok, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return len(v), nil
}

// IncrBy adds delta to the integer stored at key and returns the new value.
// A missing key is treated as 0. Errors if the value is not a valid integer or
// is a non-string type.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	var cur int64
	e, ok := getLive(sh, key, now)
	if ok {
		if e.kind != KindString {
			return 0, ErrWrongType
		}
		n, perr := parseInt64(e.str)
		if perr != nil {
			return 0, ErrNotInteger
		}
		cur = n
	} else {
		e = entry{kind: KindString}
	}
	// Overflow guard so we never wrap silently (Redis returns an error too).
	if (delta > 0 && cur > maxInt64-delta) || (delta < 0 && cur < minInt64-delta) {
		return 0, ErrOverflow
	}
	cur += delta
	e.str = formatInt64(cur)
	sh.data[key] = e
	return cur, nil
}

// ---- Generic key commands -----------------------------------------------------

// Del removes key and reports whether it existed (and was live).
func (s *Store) Del(key string) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	e, existed := sh.data[key]
	delete(sh.data, key)
	sh.mu.Unlock()
	return existed && !e.expired(now)
}

// Exists reports whether key is present and not expired (any type).
func (s *Store) Exists(key string) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	_, ok := getLive(sh, key, now)
	sh.mu.RUnlock()
	return ok
}

// Type returns the Kind of key (KindNone if missing/expired).
func (s *Store) Type(key string) Kind {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	e, ok := getLive(sh, key, now)
	sh.mu.RUnlock()
	if !ok {
		return KindNone
	}
	return e.kind
}

// Expire sets a TTL on an existing key. Returns false if the key is absent.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return false
	}
	e.expiresAt = now.Add(ttl)
	sh.data[key] = e
	return true
}

// Persist removes the TTL from key, making it permanent. Returns true if a TTL
// was actually removed.
func (s *Store) Persist(key string) bool {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	sh.data[key] = e
	return true
}

// TTL returns the remaining time-to-live for key. found is false if the key
// does not exist; hasTTL is false if the key exists but has no expiry. This lets
// the command layer distinguish Redis's -2 (no key) from -1 (no TTL).
func (s *Store) TTL(key string) (remaining time.Duration, found, hasTTL bool) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	e, ok := getLive(sh, key, now)
	sh.mu.RUnlock()
	if !ok {
		return 0, false, false
	}
	if e.expiresAt.IsZero() {
		return 0, true, false
	}
	return time.Until(e.expiresAt), true, true
}

// Keys returns all live keys matching the glob pattern. This walks every shard,
// so it is O(N) — fine for a demo/dev tool, and it is exactly what Redis KEYS
// does (with the same "don't run this in production" caveat).
func (s *Store) Keys(pattern string) []string {
	now := time.Now()
	var out []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			if globMatch(pattern, k) {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// FlushDB removes every key from every shard.
func (s *Store) FlushDB() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]entry)
		sh.mu.Unlock()
	}
}

// DBSize returns the number of live keys across all shards.
func (s *Store) DBSize() int {
	now := time.Now()
	total := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, e := range sh.data {
			if !e.expired(now) {
				total++
			}
		}
		sh.mu.RUnlock()
	}
	return total
}

// ---- Hash commands ------------------------------------------------------------

// HSet sets field=value inside the hash at key (creating the hash if needed) and
// returns the number of NEW fields added (fields that already existed count 0).
func (s *Store) HSet(key string, pairs [][2]string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		e = entry{kind: KindHash, hash: make(map[string]string)}
	} else if e.kind != KindHash {
		return 0, ErrWrongType
	}
	added := 0
	for _, p := range pairs {
		if _, exists := e.hash[p[0]]; !exists {
			added++
		}
		e.hash[p[0]] = p[1]
	}
	sh.data[key] = e
	return added, nil
}

// HGet returns the value of a single field. ok is false if key or field missing.
func (s *Store) HGet(key, field string) (value string, ok bool, err error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, live := getLive(sh, key, now)
	if !live {
		return "", false, nil
	}
	if e.kind != KindHash {
		return "", false, ErrWrongType
	}
	v, ok := e.hash[field]
	return v, ok, nil
}

// HDel removes fields from the hash at key and returns how many were removed.
func (s *Store) HDel(key string, fields []string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return 0, nil
	}
	if e.kind != KindHash {
		return 0, ErrWrongType
	}
	removed := 0
	for _, f := range fields {
		if _, exists := e.hash[f]; exists {
			delete(e.hash, f)
			removed++
		}
	}
	// If the hash is now empty, drop the key entirely (Redis does this).
	if len(e.hash) == 0 {
		delete(sh.data, key)
	} else {
		sh.data[key] = e
	}
	return removed, nil
}

// HGetAll returns field/value pairs flattened as [f1, v1, f2, v2, ...].
func (s *Store) HGetAll(key string) ([]string, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return nil, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(e.hash)*2)
	for f, v := range e.hash {
		out = append(out, f, v)
	}
	return out, nil
}

// HKeys returns all field names in the hash at key.
func (s *Store) HKeys(key string) ([]string, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return nil, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(e.hash))
	for f := range e.hash {
		out = append(out, f)
	}
	return out, nil
}

// HVals returns all values in the hash at key.
func (s *Store) HVals(key string) ([]string, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return nil, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(e.hash))
	for _, v := range e.hash {
		out = append(out, v)
	}
	return out, nil
}

// HLen returns the number of fields in the hash at key (0 if missing).
func (s *Store) HLen(key string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return 0, nil
	}
	if e.kind != KindHash {
		return 0, ErrWrongType
	}
	return len(e.hash), nil
}

// HExists reports whether field is present in the hash at key.
func (s *Store) HExists(key, field string) (bool, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return false, nil
	}
	if e.kind != KindHash {
		return false, ErrWrongType
	}
	_, exists := e.hash[field]
	return exists, nil
}

// ---- List commands ------------------------------------------------------------

// LPush prepends values (left) to the list at key and returns the new length.
// Redis semantics: LPUSH a b c on an empty list yields [c, b, a].
func (s *Store) LPush(key string, values []string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		e = entry{kind: KindList}
	} else if e.kind != KindList {
		return 0, ErrWrongType
	}
	// Prepend each value in order, so the last arg ends up at the head.
	for _, v := range values {
		e.list = append([]string{v}, e.list...)
	}
	sh.data[key] = e
	return len(e.list), nil
}

// RPush appends values (right) to the list at key and returns the new length.
func (s *Store) RPush(key string, values []string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		e = entry{kind: KindList}
	} else if e.kind != KindList {
		return 0, ErrWrongType
	}
	e.list = append(e.list, values...)
	sh.data[key] = e
	return len(e.list), nil
}

// LPop removes and returns the head element. ok is false if the list is empty.
func (s *Store) LPop(key string) (string, bool, error) {
	return s.pop(key, true)
}

// RPop removes and returns the tail element. ok is false if the list is empty.
func (s *Store) RPop(key string) (string, bool, error) {
	return s.pop(key, false)
}

func (s *Store) pop(key string, left bool) (string, bool, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return "", false, nil
	}
	if e.kind != KindList {
		return "", false, ErrWrongType
	}
	if len(e.list) == 0 {
		return "", false, nil
	}
	var v string
	if left {
		v = e.list[0]
		e.list = e.list[1:]
	} else {
		v = e.list[len(e.list)-1]
		e.list = e.list[:len(e.list)-1]
	}
	// Drop the key when the list becomes empty (Redis behaviour).
	if len(e.list) == 0 {
		delete(sh.data, key)
	} else {
		sh.data[key] = e
	}
	return v, true, nil
}

// LLen returns the length of the list at key (0 if missing).
func (s *Store) LLen(key string) (int, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return 0, nil
	}
	if e.kind != KindList {
		return 0, ErrWrongType
	}
	return len(e.list), nil
}

// LIndex returns the element at index (supports negative indexes from the tail).
func (s *Store) LIndex(key string, index int) (string, bool, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return "", false, nil
	}
	if e.kind != KindList {
		return "", false, ErrWrongType
	}
	i := normalizeIndex(index, len(e.list))
	if i < 0 || i >= len(e.list) {
		return "", false, nil
	}
	return e.list[i], true, nil
}

// LRange returns the elements between start and stop (inclusive), supporting
// negative indexes and Redis-style clamping to the list bounds.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	sh := s.shardFor(key)
	now := time.Now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := getLive(sh, key, now)
	if !ok {
		return nil, nil
	}
	if e.kind != KindList {
		return nil, ErrWrongType
	}
	n := len(e.list)
	lo := normalizeIndex(start, n)
	hi := normalizeIndex(stop, n)
	if lo < 0 {
		lo = 0
	}
	if hi >= n {
		hi = n - 1
	}
	if lo > hi || n == 0 {
		return []string{}, nil
	}
	// Copy so callers can't mutate the shard's backing slice without the lock.
	out := make([]string, hi-lo+1)
	copy(out, e.list[lo:hi+1])
	return out, nil
}

// ---- Maintenance --------------------------------------------------------------

// SweepExpired scans every shard once and deletes expired keys. Call this
// periodically from a background goroutine to reclaim memory for keys that are
// never read again (Redis calls this "active expiration").
func (s *Store) SweepExpired() (removed int) {
	now := time.Now()
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, e := range sh.data {
			if e.expired(now) {
				delete(sh.data, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// Snapshot represents one live key's full state, used by AOF rewrite to emit a
// compact set of commands that recreates the current dataset.
type Snapshot struct {
	Key       string
	Kind      Kind
	Str       string
	Hash      map[string]string
	List      []string
	ExpiresAt time.Time // zero == no expiry
}

// SnapshotAll returns a point-in-time copy of every live key. Each shard is
// locked in turn (not all at once), so this is a "fuzzy" snapshot — good enough
// for AOF rewrite, which only needs to capture a consistent-per-key view.
func (s *Store) SnapshotAll() []Snapshot {
	now := time.Now()
	var out []Snapshot
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			snap := Snapshot{Key: k, Kind: e.kind, ExpiresAt: e.expiresAt}
			switch e.kind {
			case KindString:
				snap.Str = e.str
			case KindHash:
				snap.Hash = make(map[string]string, len(e.hash))
				for f, v := range e.hash {
					snap.Hash[f] = v
				}
			case KindList:
				snap.List = append([]string(nil), e.list...)
			}
			out = append(out, snap)
		}
		sh.mu.RUnlock()
	}
	return out
}

func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// normalizeIndex converts a possibly-negative Redis index into a 0-based index.
// -1 means the last element, -2 the second to last, etc.
func normalizeIndex(i, length int) int {
	if i < 0 {
		return length + i
	}
	return i
}
