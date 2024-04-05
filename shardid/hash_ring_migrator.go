package shardid

import "slices"

// HRMigrator scale out database instances
type HRMigrator struct {
	r1 *HashRing
	r2 *HashRing

	affectedDbs    []int
	affectedVNodes map[uint32]bool
}

func NewHRMigrator(r1 *HashRing, r2 *HashRing) *HRMigrator {
	m := &HRMigrator{
		r1: r1,
		r2: r2,
	}

	m.affectedVNodes = make(map[uint32]bool)

	m.apply()

	return m
}

// Has check if v should be migrated from old database to new one
func (m *HRMigrator) Has(v string) bool {
	_, n := m.r1.Locate(v)
	return m.affectedVNodes[n]
}

func (m *HRMigrator) apply() {
	var (
		db1 int
		db2 int
	)

	dbs := make(map[int]bool)

	for _, v := range m.r1.vNodes {
		db1 = m.r1.getPreviousDB(v)
		db2 = m.r2.getPreviousDB(v)

		if db1 != db2 { // the node's previous db is changed, data should be checked if it should be migrated to previous db
			dbs[m.r1.dbs[v]] = true
			m.affectedVNodes[v] = true
		}
	}

	if len(dbs) > 0 {
		for k := range dbs {
			m.affectedDbs = append(m.affectedDbs, k)
		}
		slices.Sort(m.affectedDbs)
	}
}
