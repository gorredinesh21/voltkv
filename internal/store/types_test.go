package store

import (
	"sort"
	"testing"
	"time"
)

func TestIncrDecr(t *testing.T) {
	s := New(4)
	if v, err := s.IncrBy("n", 1); err != nil || v != 1 {
		t.Fatalf("incr fresh: got %d,%v want 1,nil", v, err)
	}
	if v, _ := s.IncrBy("n", 9); v != 10 {
		t.Fatalf("incr: got %d want 10", v)
	}
	if v, _ := s.IncrBy("n", -4); v != 6 {
		t.Fatalf("decr: got %d want 6", v)
	}
	// Non-numeric string should error.
	s.Set("s", "abc", 0)
	if _, err := s.IncrBy("s", 1); err != ErrNotInteger {
		t.Fatalf("expected ErrNotInteger, got %v", err)
	}
}

func TestAppendGetSetStrLen(t *testing.T) {
	s := New(4)
	if n, _ := s.Append("k", "foo"); n != 3 {
		t.Fatalf("append: got %d want 3", n)
	}
	if n, _ := s.Append("k", "bar"); n != 6 {
		t.Fatalf("append2: got %d want 6", n)
	}
	if n, _ := s.StrLen("k"); n != 6 {
		t.Fatalf("strlen: got %d want 6", n)
	}
	old, existed, _ := s.GetSet("k", "new")
	if !existed || old != "foobar" {
		t.Fatalf("getset: got %q,%v want foobar,true", old, existed)
	}
	if v, _, _ := s.Get("k"); v != "new" {
		t.Fatalf("getset value: got %q want new", v)
	}
}

func TestWrongType(t *testing.T) {
	s := New(4)
	s.Set("str", "v", 0)
	if _, err := s.LPush("str", []string{"x"}); err != ErrWrongType {
		t.Fatalf("LPush on string: got %v want ErrWrongType", err)
	}
	if _, err := s.HSet("str", [][2]string{{"f", "v"}}); err != ErrWrongType {
		t.Fatalf("HSet on string: got %v want ErrWrongType", err)
	}
	if _, _, err := s.Get("str"); err != nil {
		t.Fatalf("Get on string should be fine, got %v", err)
	}
	_, _ = s.LPush("lst", []string{"a"})
	if _, _, err := s.Get("lst"); err != ErrWrongType {
		t.Fatalf("Get on list: got %v want ErrWrongType", err)
	}
}

func TestHash(t *testing.T) {
	s := New(4)
	added, _ := s.HSet("h", [][2]string{{"a", "1"}, {"b", "2"}})
	if added != 2 {
		t.Fatalf("hset added: got %d want 2", added)
	}
	// Overwriting a field adds 0 new fields.
	added, _ = s.HSet("h", [][2]string{{"a", "9"}, {"c", "3"}})
	if added != 1 {
		t.Fatalf("hset added2: got %d want 1", added)
	}
	if v, ok, _ := s.HGet("h", "a"); !ok || v != "9" {
		t.Fatalf("hget a: got %q,%v want 9,true", v, ok)
	}
	if n, _ := s.HLen("h"); n != 3 {
		t.Fatalf("hlen: got %d want 3", n)
	}
	if ex, _ := s.HExists("h", "b"); !ex {
		t.Fatal("hexists b should be true")
	}
	keys, _ := s.HKeys("h")
	sort.Strings(keys)
	if len(keys) != 3 || keys[0] != "a" {
		t.Fatalf("hkeys: got %v", keys)
	}
	removed, _ := s.HDel("h", []string{"a", "zzz"})
	if removed != 1 {
		t.Fatalf("hdel: got %d want 1", removed)
	}
	if s.Type("h") != KindHash {
		t.Fatal("type should still be hash")
	}
	// Deleting the last fields drops the key.
	_, _ = s.HDel("h", []string{"b", "c"})
	if s.Type("h") != KindNone {
		t.Fatal("empty hash key should be gone")
	}
}

func TestList(t *testing.T) {
	s := New(4)
	// RPUSH a b c -> [a b c]
	if n, _ := s.RPush("l", []string{"a", "b", "c"}); n != 3 {
		t.Fatalf("rpush: got %d want 3", n)
	}
	// LPUSH x y -> [y x a b c]
	if n, _ := s.LPush("l", []string{"x", "y"}); n != 5 {
		t.Fatalf("lpush: got %d want 5", n)
	}
	want := []string{"y", "x", "a", "b", "c"}
	got, _ := s.LRange("l", 0, -1)
	if !equal(got, want) {
		t.Fatalf("lrange: got %v want %v", got, want)
	}
	if v, ok, _ := s.LIndex("l", 0); !ok || v != "y" {
		t.Fatalf("lindex 0: got %q", v)
	}
	if v, ok, _ := s.LIndex("l", -1); !ok || v != "c" {
		t.Fatalf("lindex -1: got %q", v)
	}
	if v, _, _ := s.LPop("l"); v != "y" {
		t.Fatalf("lpop: got %q want y", v)
	}
	if v, _, _ := s.RPop("l"); v != "c" {
		t.Fatalf("rpop: got %q want c", v)
	}
	if n, _ := s.LLen("l"); n != 3 {
		t.Fatalf("llen: got %d want 3", n)
	}
	// LRANGE clamps out-of-range indexes.
	got, _ = s.LRange("l", -100, 100)
	if !equal(got, []string{"x", "a", "b"}) {
		t.Fatalf("lrange clamp: got %v", got)
	}
}

func TestExpireTTLPersist(t *testing.T) {
	s := New(4)
	s.Set("k", "v", 0)
	if _, _, hasTTL := s.TTL("k"); hasTTL {
		t.Fatal("fresh key should have no TTL")
	}
	if !s.Expire("k", 100*time.Second) {
		t.Fatal("expire on existing key should return true")
	}
	rem, found, hasTTL := s.TTL("k")
	if !found || !hasTTL || rem <= 0 || rem > 100*time.Second {
		t.Fatalf("ttl after expire: rem=%v found=%v hasTTL=%v", rem, found, hasTTL)
	}
	if !s.Persist("k") {
		t.Fatal("persist should remove the TTL and return true")
	}
	if _, _, hasTTL := s.TTL("k"); hasTTL {
		t.Fatal("TTL should be gone after persist")
	}
	// Expire on a missing key returns false.
	if s.Expire("missing", time.Second) {
		t.Fatal("expire on missing key should be false")
	}
}

func TestKeysGlobAndDBSize(t *testing.T) {
	s := New(8)
	s.Set("user:1", "a", 0)
	s.Set("user:2", "b", 0)
	s.Set("post:1", "c", 0)
	if n := s.DBSize(); n != 3 {
		t.Fatalf("dbsize: got %d want 3", n)
	}
	matches := s.Keys("user:*")
	sort.Strings(matches)
	if !equal(matches, []string{"user:1", "user:2"}) {
		t.Fatalf("keys user:*: got %v", matches)
	}
	if len(s.Keys("*")) != 3 {
		t.Fatalf("keys *: got %v", s.Keys("*"))
	}
	if len(s.Keys("user:?")) != 2 {
		t.Fatalf("keys user:?: got %v", s.Keys("user:?"))
	}
	s.FlushDB()
	if s.DBSize() != 0 {
		t.Fatal("dbsize should be 0 after flush")
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"user:*", "user:42", true},
		{"user:*", "post:42", false},
		{"h?llo", "hello", true},
		{"h?llo", "heello", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"a-z", "a-z", true},
		{"foo", "foo", true},
		{"foo", "foobar", false},
	}
	for _, c := range cases {
		if got := globMatch(c.pat, c.s); got != c.want {
			t.Errorf("globMatch(%q,%q)=%v want %v", c.pat, c.s, got, c.want)
		}
	}
}

func equal(a, b []string) bool {
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
