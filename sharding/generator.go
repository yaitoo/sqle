package sharding

import (
	"sync"
	"time"
)

type Generator struct {
	sync.Mutex
	_ noCopy // nolint: unused

	workerID    int8
	databaseNum int16
	table       TableShard
	now         func() time.Time

	lastTime       time.Time
	nextSequence   int16
	nextDatabaseID int16
}

func New(options ...Option) *Generator {
	g := &Generator{
		now:         time.Now,
		databaseNum: 1,
		table:       TableShardNone,
	}
	for _, option := range options {
		option(g)
	}
	return g
}

func (g *Generator) Next() uint64 {
	g.Lock()

	defer func() {
		g.nextSequence++
		g.Unlock()
	}()

	timeNow := g.now()
	// sequence overflow capacity
	if g.nextSequence > 2047 {
		// time move backwards, waiting system clock to move forward
		if !timeNow.After(g.lastTime) {
			g.nextSequence = 0
			timeNow = g.waitNextMillis()
		}
	} else {
		// time move backwards, use Built-in clock to move forward
		if !timeNow.After(g.lastTime) {
			timeNow = g.moveNextMillis()
		}
	}

	return Build(uint64(timeNow.UnixMilli()), g.workerID, g.getDatabaseID(), g.table, g.nextSequence)

}

func (g *Generator) getDatabaseID() uint16 {
	if g.databaseNum < 2 {
		return 0
	}

	defer func() {
		g.nextDatabaseID++
	}()

	if g.nextDatabaseID < g.databaseNum {
		return g.nextDatabaseID
	}

	g.nextDatabaseID = 0
	return 0
}

func (g *Generator) waitNextMillis() time.Time {
	last := g.now()
	for {
		if last.After(g.lastTime) {
			break
		}

		last = g.now()
	}

	g.lastTime = last

	return last
}
func (g *Generator) moveNextMillis() time.Time {
	g.lastTime = g.lastTime.Add(1 * time.Millisecond)

	return g.lastTime
}
