package sharding

import "time"

type Option func(g *Generator)

func WithWorker(i int8) Option {
	return func(g *Generator) {
		if i > 0 && i < 4 {
			g.workerID = i
		}
	}
}

func WithDatabase(num int16) Option {
	return func(g *Generator) {
		if num > 0 && num < 1024 {
			g.databaseNum = num
		}
	}
}

func WithShardTable(ts TableShard) Option {
	return func(g *Generator) {
		if ts >= TableShardNone && ts <= TableShardDay {
			g.table = ts
		}
	}
}

func WithTimeNow(now func() time.Time) Option {
	return func(g *Generator) {
		g.now = now
	}
}
