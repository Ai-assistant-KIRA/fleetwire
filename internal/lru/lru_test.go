package lru

import (
	"testing"
	"time"
)

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	c := New[string, int](2, 0)
	c.Add("a", 1)
	c.Add("b", 2)
	c.Get("a")
	c.Add("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("a should still be cached")
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d", c.Len())
	}
}

func TestExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	c := New[string, int](10, time.Minute)
	c.now = func() time.Time { return now }
	c.Add("k", 1)
	now = now.Add(59 * time.Second)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expired too early")
	}
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Fatal("not expired")
	}
	if got := c.GetOrAdd("k", func() int { return 7 }); got != 7 {
		t.Fatalf("GetOrAdd = %d", got)
	}
}

// TestRemoveExpired: expired entries leave the cache without being asked
// for; the others stay, in their order.
func TestRemoveExpired(t *testing.T) {
	now := time.Unix(0, 0)
	c := New[string, int](3, time.Minute)
	c.now = func() time.Time { return now }
	c.Add("old", 1)
	now = now.Add(30 * time.Second)
	c.Add("new", 2)
	c.Add("newer", 3)
	now = now.Add(31 * time.Second)
	if n := c.RemoveExpired(); n != 1 || c.Len() != 2 {
		t.Fatalf("RemoveExpired = %d, Len = %d; want 1 and 2", n, c.Len())
	}
	if _, ok := c.Get("new"); !ok {
		t.Fatal("an entry that had not expired was removed")
	}
	// "newer" is now the least recently used.
	c.Add("x", 4)
	c.Add("y", 5)
	if _, ok := c.Get("newer"); ok {
		t.Fatal("the eviction order did not survive RemoveExpired")
	}
	if n := New[string, int](2, 0).RemoveExpired(); n != 0 {
		t.Fatalf("a cache without expiry removed %d entries", n)
	}
}

func TestRemoveFunc(t *testing.T) {
	c := New[int, string](10, 0)
	for i := range 6 {
		c.Add(i, "v")
	}
	c.RemoveFunc(func(k int) bool { return k%2 == 0 })
	for i := range 6 {
		if _, ok := c.Get(i); ok == (i%2 == 0) {
			t.Errorf("key %d cached = %v after removing the even keys", i, ok)
		}
	}
	if c.Len() != 3 {
		t.Fatalf("Len = %d, want 3", c.Len())
	}
	// The eviction order survives: the least recently used odd key goes first.
	for i := range 8 {
		c.Add(100+i, "w")
	}
	if _, ok := c.Get(1); ok {
		t.Fatal("key 1 should have been evicted first")
	}
}
