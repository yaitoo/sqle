package sharding

import (
	"sync"
	"time"
)

type Generator struct {
	sync.Mutex
	_ noCopy // nolint: unused

	workerID      int8
	databaseTotal int16
	tableRotate   TableRotate
	now           func() time.Time

	lastMillis     int64
	nextSequence   int16
	nextDatabaseID int16
}

func New(options ...Option) *Generator {
	g := &Generator{
		now:           time.Now,
		databaseTotal: 1,
		tableRotate:   None,
	}
	for _, option := range options {
		option(g)
	}
	return g
}

func (g *Generator) Next() int64 {
	g.Lock()

	defer func() {
		g.nextSequence++
		g.Unlock()
	}()

	nowMillis := g.now().UnixMilli()

	if g.nextSequence > MaxSequence {
		// time move backwards,and sequence overflows capacity, waiting system clock to move forward
		if nowMillis < g.lastMillis {
			g.nextSequence = 0
			nowMillis = g.tillNextMillis()
		}
	} else {
		// time move backwards,but sequence doesn't overflow capacity, use Built-in clock to move forward
		if nowMillis < g.lastMillis {
			nowMillis = g.moveNextMillis()
		}
	}

	return Build(nowMillis, g.workerID, g.getNextDatabaseID(), g.tableRotate, g.nextSequence)

}

func (g *Generator) getNextDatabaseID() int16 {
	if g.databaseTotal <= 1 {
		return 0
	}

	defer func() {
		g.nextDatabaseID++
	}()

	if g.nextDatabaseID < g.databaseTotal {
		return g.nextDatabaseID
	}

	g.nextDatabaseID = 0
	return 0
}

func (g *Generator) tillNextMillis() int64 {
	lastMillis := g.now().UnixMilli()
	for {
		if lastMillis > g.lastMillis {
			break
		}

		lastMillis = g.now().UnixMilli()
	}

	g.lastMillis = lastMillis

	return lastMillis
}
func (g *Generator) moveNextMillis() int64 {
	g.lastMillis = g.lastMillis + 1

	return g.lastMillis
}
