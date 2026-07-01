package store

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSetGetDel(t *testing.T) {
	s := New(16)
	s.Set("foo", "bar", 0)
	if v, ok, err := s.Get("foo"); err != nil || !ok || v != "bar" {
		t.Fatalf("got %q,%v,%v want bar,true,nil", v, ok, err)
	}
	if !s.Del("foo") {
		t.Fatal("expected Del to report the key existed")
	}
	if _, ok, _ := s.Get("foo"); ok {
		t.Fatal("expected key to be gone after Del")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := New(4)
	s.Set("k", "v", 20*time.Millisecond)
	if _, ok, _ := s.Get("k"); !ok {
		t.Fatal("key should exist before expiry")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("key should be expired (lazy expiry on read)")
	}
}

// TestConcurrentAccess is meant to be run with -race:
//
//	go test -race ./internal/store
//
// Many goroutines hammer the store on overlapping keys; the race detector
// will flag any unsynchronized access.
func TestConcurrentAccess(t *testing.T) {
	s := New(16)
	const goroutines, ops = 64, 5000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := "key" + strconv.Itoa((g*ops+i)%1000)
				s.Set(key, "v", 0)
				s.Get(key)
				if i%7 == 0 {
					s.Del(key)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkSetShards shows why sharding matters. Run:
//
//	go test -bench BenchmarkSetShards -benchmem ./internal/store
//
// Compare ns/op across shard counts — more shards == less lock contention
// under parallel load. Put the resulting table in your README.
func BenchmarkSetShards(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			s := New(shards)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.Set("key"+strconv.Itoa(i%1000), "value", 0)
					i++
				}
			})
		})
	}
}
