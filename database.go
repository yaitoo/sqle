package sqle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

// Database abstracts the subset of *sql.DB used by sqle so that custom
// wrappers (middleware, tracing, sharding proxies, mocks) can be plugged
// in wherever sqle accepts a connection pool.
//
// *sql.DB does NOT satisfy Database directly because Begin/BeginTx
// return *sql.Tx rather than the Tx interface. The internal sqlDBWrapper
// adapts *sql.DB; Open applies it automatically when an *sql.DB is
// passed.
type Database interface {
	// Query
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	// Exec
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	// Prepare
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	// Tx — returned values satisfy the Tx interface; *sql.Tx satisfies it by default.
	Begin() (Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)

	// Conn / Driver
	Conn(ctx context.Context) (*sql.Conn, error)
	Driver() driver.Driver

	// Lifecycle
	Ping() error
	PingContext(ctx context.Context) error
	Close() error

	// Pool
	SetMaxOpenConns(n int)
	SetMaxIdleConns(n int)
	SetConnMaxLifetime(d time.Duration)
	SetConnMaxIdleTime(d time.Duration)
	Stats() sql.DBStats
}

// Tx abstracts the subset of *sql.Tx used by sqle so that custom
// transaction implementations (middleware, tracing, sharding proxies,
// mocks) can be plugged in alongside a Database.
//
// *sql.Tx satisfies this interface implicitly; callers using the
// standard library do not need to change anything.
type Tx interface {
	// Query
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	// Exec
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	// Prepare
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	// Lifecycle
	Commit() error
	Rollback() error
}

// sqlDBWrapper adapts *sql.DB to the Database interface by routing
// Begin/BeginTx results through the Tx interface.
type sqlDBWrapper struct {
	*sql.DB
}

// Begin wraps *sql.DB.Begin so the returned *sql.Tx is exposed as the
// Tx interface.
func (db *sqlDBWrapper) Begin() (Tx, error) {
	return db.DB.Begin()
}

// BeginTx wraps *sql.DB.BeginTx so the returned *sql.Tx is exposed as
// the Tx interface.
func (db *sqlDBWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	return db.DB.BeginTx(ctx, opts)
}

// Compile-time assertion that *sql.Tx satisfies Tx.
var _ Tx = (*sql.Tx)(nil)
