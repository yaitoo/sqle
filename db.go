package sqle

import (
	"database/sql"
	"errors"
	"fmt"
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
// Each argument must be either a Database implementation (custom
// middleware, tracing, mocks, etc.) or a *sql.DB (which is wrapped
// internally via sqlDBWrapper so it satisfies the Database interface).
//
// *sql.DB values are detected at runtime and wrapped automatically —
// callers using the standard library do not need to wrap them manually.
func Open(dbArgs ...any) *DB {
	dbs := make([]Database, len(dbArgs))
	for i, arg := range dbArgs {
		switch v := arg.(type) {
		case Database:
			dbs[i] = v
		case *sql.DB:
			dbs[i] = &sqlDBWrapper{DB: v}
		default:
			panic(fmt.Sprintf("sqle: Open argument %d has unsupported type %T", i, arg))
		}
	}

	return openDatabases(dbs...)
}

// openDatabases is the internal constructor used by Open and OpenDB.
func openDatabases(dbs ...Database) *DB {
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

// OpenDB is the type-safe convenience for callers using *sql.DB
// directly. It applies sqlDBWrapper to each *sql.DB so they satisfy
// the Database interface, then delegates to openDatabases.
func OpenDB(dbs ...*sql.DB) *DB {
	wrapped := make([]Database, len(dbs))
	for i, db := range dbs {
		wrapped[i] = &sqlDBWrapper{DB: db}
	}
	return openDatabases(wrapped...)
}

// Add dynamically scales out the DB with new databases. Each value
// must satisfy the Database interface; callers passing *sql.DB should
// wrap each one with sqlDBWrapper (or define a custom Database type).
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
