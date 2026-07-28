package sqle

import (
	"errors"
	"sync"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

var (
	StmtMaxIdleTime = 3 * time.Minute
	ErrMissingDHT   = errors.New("sqle: missing_dht")
)

// DB represents a database connection pool with sharding support.
type DB struct {
	*Client
	_ noCopy //nolint: unused

	mu   sync.RWMutex
	dhts map[string]*shardid.DHT
	dbs  []*Client
}

// Open creates a new DB instance with the provided database connections.
// Any value satisfying the Database interface is accepted; *sql.DB
// satisfies it out of the box. The type parameter lets callers pass
// a `[]*sql.DB` (or any other Database-conforming type) slice directly
// via `Open(dbs...)`.
func Open[T Database](dbs ...T) *DB {
	d := &DB{
		dhts: make(map[string]*shardid.DHT),
	}

	for i, db := range dbs {
		ctx := &Client{
			DB:              db,
			Index:           i,
			stmts:           make(map[string]*Stmt),
			stmtMaxIdleTime: StmtMaxIdleTime,
		}
		d.dbs = append(d.dbs, ctx)
		go ctx.checkIdleStmt()
	}

	d.Client = d.dbs[0]

	return d
}

// Add dynamically scales out the DB with new databases. Go does not
// allow type parameters on methods of non-generic types, so callers
// passing a []*sql.DB slice must convert it via a small helper (or use
// individual arguments). For typical scale-out patterns this is not a
// friction point; the primary entry point that benefits from generic
// variadic is Open.
func (db *DB) Add(dbs ...Database) {
	db.Lock()
	defer db.Unlock()

	n := len(db.dbs)

	for i, d := range dbs {
		ctx := &Client{
			DB:              d,
			Index:           n + i,
			stmts:           make(map[string]*Stmt),
			stmtMaxIdleTime: StmtMaxIdleTime,
		}
		db.dbs = append(db.dbs, ctx)
		go ctx.checkIdleStmt()
	}
}

// On selects the database context based on the shardid ID.
func (db *DB) On(id shardid.ID) *Client {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dbs[int(id.DatabaseID)]
}

// NewDHT creates a new DHT (Distributed Hash Table) with the specified databases.
func (db *DB) NewDHT(name string, dbs ...int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.dhts[name] = shardid.NewDHT(dbs...)
}

// GetDHT returns the DHT (Distributed Hash Table) with the specified name.
func (db *DB) GetDHT(name string) *shardid.DHT {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.dhts[name]
}

// OnDHT selects the database context based on the DHT (Distributed Hash Table) key.
func (db *DB) OnDHT(key string, names ...string) (*Client, error) {
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