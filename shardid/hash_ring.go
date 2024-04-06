package shardid

import (
	"hash/fnv"
	"slices"
	"strconv"
)

var (
	defaultReplicas = []string{"A", "C", "E", "G", "I", "K", "M", "O", "Q", "S"}
)

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
