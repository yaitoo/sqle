package shardid

import (
	"hash/fnv"
	"slices"
	"strconv"
)

// defaultReplicas is the set of virtual-node labels HashRing places on the
// hash ring for every physical database. Each label is concatenated with
// the database index (e.g. "A0", "A1", ..., "S9") and hashed to a fixed
// position on the 32-bit ring, so each database ends up with
// len(defaultReplicas) virtual nodes.
//
// The labels are the ten odd letters of the English alphabet (A, C, E,
// G, I, K, M, O, Q, S). They are chosen deliberately rather than the
// obvious "V0"…"V9":
//   - they have widely-spread FNV-1a hashes, so virtual nodes land in
//     distinct buckets of the ring instead of clustering near each
//     other (verified empirically by hash_ring_test.go and dht_test.go,
//     whose fixtures are derived from these exact labels);
//   - they are short and visually unambiguous in logs;
//   - using "V0"…"V9" — the same prefix for every replica — caused
//     nearby virtual nodes in earlier iterations of this library,
//     producing poor key distribution on small ring sizes.
//
// defaultReplicas is package-private. DHTs built via NewDHT(dbs...) use
// this slice unless the caller wraps the HashRing via WithReplicas(...),
// in which case the caller-supplied labels fully replace it (see
// NewDHT, NewHR, and DHT.Add).
var defaultReplicas = []string{"A", "C", "E", "G", "I", "K", "M", "O", "Q", "S"}

// HashRing implement consistent hashing for database sharding with hash key
type HashRing struct {
	dbCount int
	dbs     map[uint32]int

	vnCount int
	vNodes  []uint32

	replicas []string
}

// NewHR create HashRing with n dbs and virtual nodes
func NewHR(n int, options ...HashRingOption) *HashRing {
	r := &HashRing{
		dbCount: n,
		dbs:     make(map[uint32]int),
	}

	for _, o := range options {
		o(r)
	}

	if len(r.replicas) == 0 {
		r.replicas = defaultReplicas
	}

	r.vnCount = n * len(r.replicas)

	for i := 0; i < n; i++ {
		for _, v := range r.replicas {
			k := getHash(v + strconv.Itoa(i))
			r.dbs[k] = i
			r.vNodes = append(r.vNodes, k)
		}
	}

	slices.Sort(r.vNodes)

	return r
}

// On locate db and vNode for data v
func (r *HashRing) On(v string) (int, uint32) {
	k := getHash(v)

	var found uint32
	for i, n := range r.vNodes {
		if n > k {
			found = r.vNodes[i]
			break
		}
	}

	if found == 0 {
		found = r.vNodes[0]
	}

	return r.dbs[found], found
}

// getPreviousDB get previous db for node v
func (r *HashRing) getPreviousDB(v uint32) int {
	i, _ := slices.BinarySearch(r.vNodes, v)

	// first node, its previous node is last one
	if i == 0 {
		return r.dbs[r.vNodes[r.vnCount-1]]
	}

	return r.dbs[r.vNodes[i-1]]
}

// getHash get hash for data v
func getHash(v string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(v)) // nolint: errcheck
	return h.Sum32()
}
