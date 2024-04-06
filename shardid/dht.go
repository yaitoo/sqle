package shardid

import (
	"errors"
	"slices"
	"sync"
)

var ErrItemIsBusy = errors.New("sqle: item is busy, waiting for scaling done")

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
	m.RLock()
	defer m.RUnlock()

	i, n := m.current.On(v)

	current := m.dbs[i]

	ok := m.affectedVNodes[n]
	if ok {
		n, _ := m.next.On(v)
		if n == i {
			return current, current, nil
		}

		return current, m.dbs[n], ErrItemIsBusy
	}

	return current, current, nil
}

// Done dbs are added, then reset current/next HashRing
func (m *DHT) Done() {
	m.Lock()
	defer m.Unlock()

	m.affectedDbs = nil
	m.affectedVNodes = make(map[uint32]bool)
	m.current = m.next
	m.next = nil
}

// Add dynamically add databases, and return affected database
func (m *DHT) Add(dbs ...int) []int {
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
