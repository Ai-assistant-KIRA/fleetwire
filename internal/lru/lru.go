// Package lru is a small size-bounded cache with optional expiry, safe for
// concurrent use. It bounds the memory of per-token rate limiters and the
// unwrapped data-key cache.
package lru

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key     K
	value   V
	expires time.Time
}

// Cache holds at most size entries; the least recently used is evicted.
type Cache[K comparable, V any] struct {
	mu    sync.Mutex
	size  int
	ttl   time.Duration
	now   func() time.Time
	order *list.List
	items map[K]*list.Element
}

// New returns a cache. ttl = 0 means entries do not expire.
func New[K comparable, V any](size int, ttl time.Duration) *Cache[K, V] {
	return &Cache[K, V]{size: size, ttl: ttl, now: time.Now, order: list.New(), items: make(map[K]*list.Element)}
}

// at returns the entry stored in el. Only *entry values are ever stored.
func at[K comparable, V any](el *list.Element) *entry[K, V] {
	e, _ := el.Value.(*entry[K, V])
	return e
}

// Get returns the value for key if present and not expired.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	e := at[K, V](el)
	if c.ttl > 0 && c.now().After(e.expires) {
		c.order.Remove(el)
		delete(c.items, key)
		return zero, false
	}
	c.order.MoveToFront(el)
	return e.value, true
}

// Add stores value for key.
func (c *Cache[K, V]) Add(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addLocked(key, value)
}

func (c *Cache[K, V]) addLocked(key K, value V) {
	exp := c.now().Add(c.ttl)
	if el, ok := c.items[key]; ok {
		e := at[K, V](el)
		e.value, e.expires = value, exp
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&entry[K, V]{key: key, value: value, expires: exp})
	for c.order.Len() > c.size {
		old := c.order.Back()
		c.order.Remove(old)
		delete(c.items, at[K, V](old).key)
	}
}

// GetOrAdd returns the cached value for key, or stores and returns the
// result of create. create runs under the cache lock, so it must be cheap.
func (c *Cache[K, V]) GetOrAdd(key K, create func() V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		if e := at[K, V](el); c.ttl == 0 || !c.now().After(e.expires) {
			c.order.MoveToFront(el)
			return e.value
		}
	}
	v := create()
	c.addLocked(key, v)
	return v
}

// Remove deletes key.
func (c *Cache[K, V]) Remove(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.order.Remove(el)
		delete(c.items, key)
	}
}

// RemoveFunc deletes every key for which match returns true. It visits
// every entry, so it suits rare clean-ups, not a hot path.
func (c *Cache[K, V]) RemoveFunc(match func(K) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, el := range c.items {
		if match(key) {
			c.order.Remove(el)
			delete(c.items, key)
		}
	}
}

// Len is the number of entries.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
