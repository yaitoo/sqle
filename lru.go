package sqle

import (
	"container/list"
	"sync"
)

// lruCache is a thread-safe fixed-capacity LRU cache.
//
// Entries are evicted in least-recently-used order once capacity is exceeded.
// All operations are O(1).
type lruCache[K comparable, V any] struct {
	mu      sync.Mutex
	cap     int
	order   *list.List
	entries map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key K
	val V
}

func newLRUCache[K comparable, V any](capacity int) *lruCache[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &lruCache[K, V]{
		cap:     capacity,
		order:   list.New(),
		entries: make(map[K]*list.Element, capacity),
	}
}

// Get returns the cached value for key and reports whether it was present.
// Hits mark the entry as most-recently-used.
func (c *lruCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.entries[key]
	if !ok {
		return zero, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*lruEntry[K, V]).val, true
}

// Put inserts or updates the value for key, evicting the least-recently-used
// entry if the cache is at capacity.
func (c *lruCache[K, V]) Put(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		el.Value.(*lruEntry[K, V]).val = val
		c.order.MoveToFront(el)
		return
	}
	el := c.order.PushFront(&lruEntry[K, V]{key: key, val: val})
	c.entries[key] = el
	for c.order.Len() > c.cap {
		oldest := c.order.Back()
		if oldest == nil {
			return
		}
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*lruEntry[K, V]).key)
	}
}
