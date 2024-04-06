package sqle

import (
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

var (
	StmtMaxIdleTime = 1 * time.Minute
	ErrMissingDHT   = errors.New("sqle: DHT is missing")
)

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

// Add dynamically scale out DB with new databases
func (db *DB) Add(dbs ...*sql.DB) {
	db.Lock()
	defer db.Unlock()

	n := len(db.dbs)

	for i, d := range dbs {
		ctx := &Context{
			DB:    d,
			index: n + i,
			stmts: make(map[string]*cachedStmt),
		}
		db.dbs = append(db.dbs, ctx)
		go ctx.closeIdleStmt()
	}
}

// On select database from shardid.ID
func (db *DB) On(id shardid.ID) *Context {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dbs[int(id.DatabaseID)]
}

// NewDHT create new DTH with databases
func (db *DB) NewDHT(dbs ...int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.dht = shardid.NewDHT(dbs...)
}

// DHTAdd add new databases into current DHT
func (db *DB) DHTAdd(dbs ...int) {
	db.Lock()
	defer db.Unlock()
	if db.dht == nil {
		return
	}
	db.dht.Add(dbs...)
}

// DHTAdded get databases added on current DHT, and reload it.
func (db *DB) DHTAdded() {
	db.Lock()
	defer db.Unlock()
	if db.dht == nil {
		return
	}
	db.dht.Done()
}

// OnDHT select database from DHT
func (db *DB) OnDHT(key string) (*Context, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(db.dbs) == 1 {
		return db.dbs[0], nil
	}

	if db.dht == nil {
		return nil, ErrMissingDHT
	}

	cur, _, err := db.dht.On(key)

	if err != nil {
		return nil, err

	}
	return db.dbs[cur], nil
}
