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
	ErrDataIsBusy   = errors.New("sqle: data is busy")
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

	d.dht = shardid.NewDHT(shardid.NewHR(len(dbs)))

	return d
}

// On select database from shardid.ID
func (db *DB) On(id shardid.ID) *Context {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dbs[int(id.DatabaseID)]
}

// OnDHT select database from DHT
func (db *DB) OnDHT(key string) (*Context, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(db.dbs) == 1 {
		return db.dbs[0], nil
	}

	i, err := db.dht.On(key)

	if err != nil {
		return nil, err

	}
	return db.dbs[i], nil
}

// ScaleOut dynamically scale out DB/DHT with new databases
func (db *DB) ScaleOut(dbs ...*sql.DB) {
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

	db.dht.ScaleTo(shardid.NewHR(len(db.dbs)))
}

// EndScale end scale out, and reload DHT
func (db *DB) EndScale() {
	db.Lock()
	defer db.Unlock()

	db.dht.EndScale()
}
