package shardid

import (
	"time"
)

type Option func(g *Generator)

func WithWorkerID(i int8) Option {
	return func(g *Generator) {
		if i >= 0 && i <= MaxWorkerID {
			g.workerID = i
		}
	}
}

func WithDatabase(total int16) Option {
	return func(g *Generator) {
		if total >= 0 && total <= MaxDatabaseID {
			g.databaseTotal = total
		}
	}
}

func WithTableRotate(ts TableRotate) Option {
	return func(g *Generator) {
		if ts >= NoRotate && ts <= DailyRotate {
			g.tableRotate = ts
		}
	}
}

func WithTimeNow(now func() time.Time) Option {
	return func(g *Generator) {
		g.now = now
	}
}
