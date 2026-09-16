package shardid

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrDataItemIsBusy = errors.New("sqle: data_item_is_busy")
	ErrNilDHT         = errors.New("sqle: dht_is_nil")
)

// DHT distributed hash table
type DHT struct {
	sync.RWMutex
	current *HashRing
	next    *HashRing

	dbsCount int
	dbs      map[int]int

	affectedDbs    []int
	affectedVNodes map[uint32]bool
}

// NewDHT create a distributed hash table between databases
func NewDHT(dbs ...int) *DHT {
	m := &DHT{
		dbs:            map[int]int{},
		dbsCount:       len(dbs),
		affectedVNodes: make(map[uint32]bool),
	}

	for i, db := range dbs {
		m.dbs[i] = db
	}

	m.current = NewHR(m.dbsCount, WithReplicas(defaultReplicas...))

	return m
}

// On locate database with v from current/next HashRing, return ErrItemIsBusy if it is on affected database
func (m *DHT) On(v string) (int, int, error) {
	if m == nil {
		return 0, 0, ErrNilDHT
	}
	m.RLock()
	defer m.RUnlock()

	// Route both the current-ring and (when v sits on an affected
	// virtual node) the next-ring lookup through ringOn, rather than
	// calling HashRing.On directly via m.current / m.next. This makes
	// two things explicit at the call site that the inline form hid:
	//
	//   1. No second DHT.RWMutex acquisition. The caller already holds
	//      m's RLock. HashRing.On is pure (no locking of its own) and
	//      ringOn does not change that — but if HashRing.On ever grew
	//      its own mutex in the future, funnelling both lookups through
	//      ringOn keeps the contract in one place instead of two.
	//
	//   2. The inner `n, _ := m.next.On(v)` previously shadowed the
	//      outer `n` (the vNode hash) and re-used the name for the
	//      next-ring db index. That made the conditional read like a
	//      vNode-hash comparison when it is actually a db-index
	//      comparison; ringOn gives the next-ring result its own name
	//      so the comparison is unambiguous.
	curIdx, vn := ringOn(m.current, v)
	current := m.dbs[curIdx]

	if !m.affectedVNodes[vn] {
		return current, current, nil
	}

	nextIdx, _ := ringOn(m.next, v)
	if nextIdx == curIdx {
		return current, current, nil
	}

	return current, m.dbs[nextIdx], ErrDataItemIsBusy
}

// ringOn locates v in r and returns (dbIndex, vNodeHash). It is a thin
// pass-through to HashRing.On, factored out of DHT.On so both lookups
// inside DHT.On go through one named entry point. HashRing.On takes
// no locks, so ringOn is safe to call while holding m.RLock() — see
// the comment on DHT.On for why we route through here rather than
// calling r.On directly.
func ringOn(r *HashRing, v string) (int, uint32) {
	return r.On(v)
}

// Done dbs are added, then reset current/next HashRing
func (m *DHT) Done() {
	if m == nil {
		return
	}
	m.Lock()
	defer m.Unlock()

	m.affectedDbs = nil
	m.affectedVNodes = make(map[uint32]bool)
	m.current = m.next
	m.next = nil
}

// Add dynamically add databases, and return affected database
func (m *DHT) Add(dbs ...int) []int {
	if m == nil {
		return nil
	}
	m.Lock()
	defer m.Unlock()

	for i, db := range dbs {
		m.dbs[m.dbsCount+i] = db
	}

	m.dbsCount += len(dbs)
	m.next = NewHR(m.dbsCount, WithReplicas(defaultReplicas...))
	var (
		db1 int
		db2 int
	)

	affectedDbs := make(map[int]bool)

	for _, v := range m.current.vNodes {
		db1 = m.current.getPreviousDB(v)
		db2 = m.next.getPreviousDB(v)

		if db1 != db2 { // the node's previous db is changed, data should be checked if it should be migrated to previous db
			affectedDbs[m.current.dbs[v]] = true
			m.affectedVNodes[v] = true
		}
	}

	if len(affectedDbs) > 0 {
		for k := range affectedDbs {
			m.affectedDbs = append(m.affectedDbs, k)
		}
		slices.Sort(m.affectedDbs)
	}

	return m.affectedDbs
}
