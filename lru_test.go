package sqle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUCache_BasicGetPut(t *testing.T) {
	c := newLRUCache[string, int](2)

	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss for absent key")
	}

	c.Put("a", 1)
	c.Put("b", 2)

	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("expected (1,true) for a, got (%v,%v)", v, ok)
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("expected (2,true) for b, got (%v,%v)", v, ok)
	}
}

func TestLRUCache_EvictsLeastRecentlyUsed(t *testing.T) {
	c := newLRUCache[string, int](2)

	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3) // should evict "a"

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected a to be evicted")
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("expected b to remain, got (%v,%v)", v, ok)
	}
	if v, ok := c.Get("c"); !ok || v != 3 {
		t.Fatalf("expected c to remain, got (%v,%v)", v, ok)
	}
}

func TestLRUCache_GetRefreshesRecency(t *testing.T) {
	c := newLRUCache[string, int](2)

	c.Put("a", 1)
	c.Put("b", 2)

	// Touch a so b becomes least-recently-used.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a to be present")
	}

	c.Put("c", 3) // should evict b, not a

	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("expected a to remain after refresh, got (%v,%v)", v, ok)
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if v, ok := c.Get("c"); !ok || v != 3 {
		t.Fatalf("expected c to remain, got (%v,%v)", v, ok)
	}
}

func TestLRUCache_UpdateExistingKey(t *testing.T) {
	c := newLRUCache[string, int](2)

	c.Put("a", 1)
	c.Put("a", 2)

	if v, ok := c.Get("a"); !ok || v != 2 {
		t.Fatalf("expected updated value (2), got (%v,%v)", v, ok)
	}
}

func TestLRUCache_BoundsUnboundedGrowth(t *testing.T) {
	const cap = 4
	c := newLRUCache[int, int](cap)

	for i := 0; i < 1000; i++ {
		c.Put(i, i)
	}

	// The cache must never exceed its declared capacity.
	if got := c.order.Len(); got > cap {
		t.Fatalf("cache size %d exceeds capacity %d", got, cap)
	}
}

func TestLRUCache_IntegerKeyType(t *testing.T) {
	c := newLRUCache[int, string](3)
	c.Put(1, "one")
	c.Put(2, "two")
	c.Put(3, "three")
	c.Put(4, "four") // evicts 1

	if _, ok := c.Get(1); ok {
		t.Fatal("expected 1 to be evicted")
	}
	if v, ok := c.Get(2); !ok || v != "two" {
		t.Fatalf("expected two, got (%v,%v)", v, ok)
	}
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	c := newLRUCache[int, int](64)
	done := make(chan struct{})

	for g := 0; g < 8; g++ {
		go func(base int) {
			for i := 0; i < 1000; i++ {
				key := base*1000 + i
				c.Put(key, key)
				if i%2 == 0 {
					_, _ = c.Get(key)
				}
			}
			done <- struct{}{}
		}(g)
	}

	for g := 0; g < 8; g++ {
		<-done
	}

	// Sanity: the cache must remain bounded.
	require.LessOrEqual(t, c.order.Len(), 64)
}