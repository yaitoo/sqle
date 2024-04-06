package sqle

import (
	"database/sql"
	"sync"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

var StmtMaxIdleTime = 1 * time.Minute

type DB struct {
	*Context
	_ noCopy //nolint: unused

	mu  sync.RWMutex
	dht *shardid.DHT
	dbs []*Context
}

func Open(dbs ...*sql.DB) *DB {
	d := &DB{
		Context: &Context{
			DB:    dbs[0],
			stmts: make(map[string]*cachedStmt),
		},
	}

	for i, db := range dbs {
		ctx := &Context{
			DB:    db,
			index: i,
			stmts: make(map[string]*cachedStmt),
		}
		d.dbs = append(d.dbs, ctx)
		go ctx.closeIdleStmt()
	}

	return d
}

// On select database from shardid.ID
func (db *DB) On(id shardid.ID) *Context {
	return db.dbs[int(id.DatabaseID)]
}

// OnHR select database from HashRing
func (db *DB) OnHR(key string) *Context {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(db.dbs) == 1 {
		return db.dbs[0]
	}

}
