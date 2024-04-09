package sqle

import (
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

var (
	StmtMaxIdleTime = 3 * time.Minute
	ErrMissingDHT   = errors.New("sqle: missing_dht")
)

type DB struct {
	*Context
	_ noCopy //nolint: unused

	mu   sync.RWMutex
	dhts map[string]*shardid.DHT
	dbs  []*Context
}

func Open(dbs ...*sql.DB) *DB {
	d := &DB{
		Context: &Context{
			DB:              dbs[0],
			stmts:           make(map[string]*Stmt),
			Index:           0,
			stmtMaxIdleTime: StmtMaxIdleTime,
		},
		dhts: make(map[string]*shardid.DHT),
	}

	for i, db := range dbs {
		ctx := &Context{
			DB:    db,
			Index: i,
			stmts: make(map[string]*Stmt),
		}
		d.dbs = append(d.dbs, ctx)
		go ctx.checkIdleStmt()
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
			Index: n + i,
			stmts: make(map[string]*Stmt),
		}
		db.dbs = append(db.dbs, ctx)
		go ctx.checkIdleStmt()
	}
}

// On select database from shardid.ID
func (db *DB) On(id shardid.ID) *Context {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dbs[int(id.DatabaseID)]
}

// NewDHT create new DTH with databases
func (db *DB) NewDHT(name string, dbs ...int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.dhts[name] = shardid.NewDHT(dbs...)
}

func (db *DB) GetDHT(name string) *shardid.DHT {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dhts[name]
}

// OnDHT select database from DHT
func (db *DB) OnDHT(key string, names ...string) (*Context, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var name string
	if len(names) > 0 {
		name = names[0]
	}

	dht, ok := db.dhts[name]
	if !ok {
		return nil, ErrMissingDHT
	}

	cur, _, err := dht.On(key)

	if err != nil {
		return nil, err

	}
	return db.dbs[cur], nil
}
