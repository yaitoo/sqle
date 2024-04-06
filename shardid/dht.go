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

	affectedDbs    []int
	affectedVNodes map[uint32]bool
}

// NewDHT create a distributed hash table from the HashRing
func NewDHT(current *HashRing) *DHT {
	m := &DHT{
		current: current,
	}

	m.affectedVNodes = make(map[uint32]bool)

	return m
}

// On locate database with v from current HashRing, return ErrDataIsBusy if it is migrating on scaling
func (m *DHT) On(v string) (int, error) {
	m.RLock()
	defer m.RUnlock()
	if m.current.dbCount == 1 {
		return 0, nil
	}

	i, n := m.current.On(v)

	ok := m.affectedVNodes[n]
	if ok {
		return i, ErrItemIsBusy
	}

	return i, nil
}

// EndScale end scale out, and reset current and next HashRings
func (m *DHT) EndScale() {
	m.Lock()
	defer m.Unlock()

	m.affectedDbs = nil
	m.affectedVNodes = make(map[uint32]bool)
	m.current = m.next
	m.next = nil

}

// ScaleTo scale out to new HashRing, and return affected databases
func (m *DHT) ScaleTo(next *HashRing) []int {
	m.Lock()
	defer m.Unlock()

	m.next = next

	var (
		db1 int
		db2 int
	)

	dbs := make(map[int]bool)

	for _, v := range m.current.vNodes {
		db1 = m.current.getPreviousDB(v)
		db2 = m.next.getPreviousDB(v)

		if db1 != db2 { // the node's previous db is changed, data should be checked if it should be migrated to previous db
			dbs[m.current.dbs[v]] = true
			m.affectedVNodes[v] = true
		}
	}

	if len(dbs) > 0 {
		for k := range dbs {
			m.affectedDbs = append(m.affectedDbs, k)
		}
		slices.Sort(m.affectedDbs)
	}

	return m.affectedDbs
}
