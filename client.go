package sqle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"sync"
	"time"
)

type Client struct {
	DB Database
	sync.Mutex
	_ noCopy

	stmts      map[string]*Stmt
	stmtsMutex sync.Mutex

	stmtMaxIdleTime time.Duration
	Index           int
}

// Ping forwards to the underlying Database. Kept so *DB keeps the
// promoted method set that used to come from *sql.DB embedding.
func (db *Client) Ping() error {
	return db.DB.Ping()
}

// PingContext forwards to the underlying Database.
func (db *Client) PingContext(ctx context.Context) error {
	return db.DB.PingContext(ctx)
}

// Close forwards to the underlying Database.
func (db *Client) Close() error {
	return db.DB.Close()
}

// Conn forwards to the underlying Database.
func (db *Client) Conn(ctx context.Context) (*sql.Conn, error) {
	return db.DB.Conn(ctx)
}

// Driver forwards to the underlying Database.
func (db *Client) Driver() driver.Driver {
	return db.DB.Driver()
}

// Prepare forwards to the underlying Database, returning a raw *sql.Stmt.
func (db *Client) Prepare(query string) (*sql.Stmt, error) {
	return db.DB.Prepare(query)
}

// PrepareContext forwards to the underlying Database, returning a raw *sql.Stmt.
func (db *Client) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.DB.PrepareContext(ctx, query)
}

// SetMaxOpenConns forwards to the underlying Database.
func (db *Client) SetMaxOpenConns(n int) {
	db.DB.SetMaxOpenConns(n)
}

// SetMaxIdleConns forwards to the underlying Database.
func (db *Client) SetMaxIdleConns(n int) {
	db.DB.SetMaxIdleConns(n)
}

// SetConnMaxLifetime forwards to the underlying Database.
func (db *Client) SetConnMaxLifetime(d time.Duration) {
	db.DB.SetConnMaxLifetime(d)
}

// SetConnMaxIdleTime forwards to the underlying Database.
func (db *Client) SetConnMaxIdleTime(d time.Duration) {
	db.DB.SetConnMaxIdleTime(d)
}

// Stats forwards to the underlying Database.
func (db *Client) Stats() sql.DBStats {
	return db.DB.Stats()
}

func (db *Client) Query(query string, args ...any) (*Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *Client) QueryBuilder(ctx context.Context, b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

func (db *Client) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	var rows *sql.Rows
	var stmt *Stmt
	var err error
	if len(args) > 0 {
		stmt, err = db.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			stmt.Reuse()
			return nil, err
		}

	} else {
		rows, err = db.DB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
	}

	return &Rows{Rows: rows, stmt: stmt, query: query}, nil
}

func (db *Client) QueryRow(query string, args ...any) *Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *Client) QueryRowBuilder(ctx context.Context, b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err:   err,
			query: query,
		}
	}

	return db.QueryRowContext(ctx, query, args...)
}

func (db *Client) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	var rows *sql.Rows
	var stmt *Stmt
	var err error

	if len(args) > 0 {
		stmt, err = db.prepareStmt(ctx, query)
		if err != nil {
			return &Row{
				err:   err,
				stmt:  stmt,
				query: query,
			}
		}
		rows, err = stmt.QueryContext(ctx, args...)
		return &Row{
			err:   err,
			stmt:  stmt,
			query: query,

			rows: rows,
		}
	}

	rows, err = db.DB.QueryContext(ctx, query, args...)
	return &Row{
		rows:  rows,
		stmt:  stmt,
		err:   err,
		query: query,
	}
}

func (db *Client) Exec(query string, args ...any) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *Client) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

func (db *Client) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if len(args) > 0 {
		stmt, err := db.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		defer stmt.Reuse()

		return stmt.ExecContext(ctx, args...)
	}
	return db.DB.ExecContext(context.Background(), query, args...)
}

func (db *Client) Begin(opts *sql.TxOptions) (*TxContext, error) {
	return db.BeginTx(context.TODO(), opts)

}

func (db *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*TxContext, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &TxContext{Tx: tx, stmts: make(map[string]*sql.Stmt)}, nil
}

func (db *Client) Transaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, tx *TxContext) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	err = fn(ctx, tx)
	defer func() {
		if err != nil {
			if e := tx.Rollback(); e != nil {
				log.Println("sqle: rollback ", e)
			}
		}
	}()

	if err != nil {
		return err
	}

	err = tx.Commit()

	return err
}