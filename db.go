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
	// ErrInvalidShardID is returned by DB.On when id.DatabaseID does not
	// index into the current sharded DB pool (e.g. an ID generated for a
	// larger topology is reused against a smaller one, or a forged ID is
	// passed). It replaces the previous index-out-of-range panic so callers
	// can detect topology mismatch and recover.
	ErrInvalidShardID = errors.New("sqle: invalid_shard_id")
)

// DB represents a database connection pool with sharding support.
type DB struct {
	*Client
	_ noCopy //nolint: unused

	// mu guards dhts and dbs, so that the shard pool and the DHT rings
	// are always read under the same lock.
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
		d.dbs = append(d.dbs, newClient(db, i))
	}

	d.Client = d.dbs[0]

	return d
}

// newClient creates a Client for db and starts its background statement
// reaper. The reaper runs until Client.Stop (or Client.Close) is called.
func newClient(db Database, index int) *Client {
	c := &Client{
		DB:              db,
		Index:           index,
		stmts:           make(map[string]*Stmt),
		stmtMaxIdleTime: StmtMaxIdleTime,
		done:            make(chan struct{}),
	}

	go c.checkIdleStmt()

	return c
}

// Add dynamically scales out the DB with new databases. Go does not
// allow type parameters on methods of non-generic types, so callers
// passing a []*sql.DB slice must convert it via a small helper (or use
// individual arguments). For typical scale-out patterns this is not a
// friction point; the primary entry point that benefits from generic
// variadic is Open.
func (db *DB) Add(dbs ...Database) {
	db.mu.Lock()
	defer db.mu.Unlock()

	n := len(db.dbs)

	for i, d := range dbs {
		db.dbs = append(db.dbs, newClient(d, n+i))
	}
}

// snapshotClients returns a copy of the shard pool, taken under DB.mu. A
// query captures the shards it fans out over when it is built, so the copy
// keeps a concurrent Add from tearing the slice while it is read.
func (db *DB) snapshotClients() []*Client {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dbs := make([]*Client, len(db.dbs))
	copy(dbs, db.dbs)

	return dbs
}

// shardClients returns the clients that Stop and Close have to release: the
// shard pool, plus the embedded client when it is not part of the pool.
//
// Open sets the embedded client to the first shard, but DB is exported, so a
// caller can build one that carries a Client without any shard entry. That
// client still owns a database and a statement reaper, and must not be left
// behind.
//
// The caller must hold db.mu.
func (db *DB) shardClients() []*Client {
	clients := make([]*Client, 0, len(db.dbs)+1)
	clients = append(clients, db.dbs...)

	if db.Client != nil && (len(db.dbs) == 0 || db.dbs[0] != db.Client) {
		clients = append(clients, db.Client)
	}

	return clients
}

// Stop terminates the background statement reaper of every shard client held
// by DB. It does not close the underlying databases, so the connection pools
// stay usable; use Close to release both.
//
// Stop is idempotent and safe for concurrent use.
func (db *DB) Stop() {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, c := range db.shardClients() {
		c.Stop()
	}
}

// Close stops the background statement reaper of every shard client and
// closes the underlying databases. The returned error joins the errors
// reported by the shard databases.
//
// The reapers are stopped in a first pass that cannot block, so a shard whose
// Database.Close stalls can no longer leave the other reapers running. The
// shard databases are then closed independently of each other, so one stalled
// close does not keep the other pools open. Close still waits for the shard
// databases to be closed, so a Database.Close that never returns delays the
// return of Close, but it does not hold back the other shards.
//
// DB.Close used to resolve to the embedded Client.Close, which only closed
// the first shard: the other shards, and their reaper goroutines, were left
// running.
func (db *DB) Close() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	clients := db.shardClients()

	// Stop every reaper first. Stop only closes the reaper's done
	// channel, so this pass never blocks.
	for _, c := range clients {
		c.Stop()
	}

	// Close the shard databases independently: Database.Close may block
	// on outstanding work, and that must not keep the other pools open.
	errs := make([]error, len(clients))

	var wg sync.WaitGroup

	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.DB.Close()
		}()
	}

	wg.Wait()

	return errors.Join(errs...)
}

// On selects the database context based on the shardid ID. It returns
// ErrInvalidShardID when id.DatabaseID does not index into the current
// sharded DB pool (negative or >= len(dbs)); callers that previously
// relied on the implicit panic on out-of-range IDs must now handle the
// error explicitly.
func (db *DB) On(id shardid.ID) (*Client, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idx := int(id.DatabaseID)
	if idx < 0 || idx >= len(db.dbs) {
		return nil, ErrInvalidShardID
	}

	return db.dbs[idx], nil
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