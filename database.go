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
// *sql.DB satisfies this interface implicitly; callers using standard
// library databases do not need to change anything. Begin/BeginTx continue
// to return *sql.Tx because there is no Tx abstraction in sqle.
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

	// Tx — sqle does not abstract Tx; callers receive the standard *sql.Tx.
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

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

// Compile-time assertion that *sql.DB satisfies Database.
var _ Database = (*sql.DB)(nil)