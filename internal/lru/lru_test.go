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
