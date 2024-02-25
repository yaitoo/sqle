package sqle

import (
	"context"
	"database/sql"
	"sync"

	"github.com/rs/zerolog/log"
)

type Context struct {
	*sql.DB
	sync.Mutex
	_ noCopy

	index      int
	stmts      map[string]*cachedStmt
	stmtsMutex sync.RWMutex
}

func (db *Context) Query(query string, args ...any) (*Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *Context) QueryBuilder(ctx context.Context, b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

func (db *Context) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	var rows *sql.Rows
	var stmt *sql.Stmt
	var err error
	if len(args) > 0 {
		stmt, err = db.prepareStmt(ctx, query)
		if err == nil {
			rows, err = stmt.QueryContext(ctx, args...)
			if err != nil {
				return nil, err
			}
		}

	} else {
		rows, err = db.DB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
	}

	return &Rows{Rows: rows, query: query}, nil
}

func (db *Context) QueryRow(query string, args ...any) *Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *Context) QueryRowBuilder(ctx context.Context, b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err:   err,
			query: query,
		}
	}

	return db.QueryRowContext(ctx, query, args...)
}

func (db *Context) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	var rows *sql.Rows
	var stmt *sql.Stmt
	var err error

	if len(args) > 0 {
		stmt, err = db.prepareStmt(ctx, query)
		if err != nil {
			return &Row{
				err:   err,
				query: query,
			}
		}
		rows, err = stmt.QueryContext(ctx, args...)
		return &Row{
			rows:  rows,
			err:   err,
			query: query,
		}
	}

	rows, err = db.DB.QueryContext(ctx, query, args...)
	return &Row{
		rows:  rows,
		err:   err,
		query: query,
	}
}

func (db *Context) Exec(query string, args ...any) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *Context) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

func (db *Context) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if len(args) > 0 {
		stmt, err := db.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		return stmt.ExecContext(ctx, args...)
	}
	return db.DB.ExecContext(context.Background(), query, args...)
}

func (db *Context) Begin(opts *sql.TxOptions) (*Tx, error) {
	return db.BeginTx(context.TODO(), opts)

}

func (db *Context) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx, cachedStmts: make(map[string]*sql.Stmt)}, nil
}

func (db *Context) Transaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, tx *Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	err = fn(ctx, tx)
	if err != nil {
		if e := tx.Rollback(); e != nil {
			log.Error().Str("pkg", "sqle").Str("tag", "tx").Err(e)
		}
		return err
	}
	return tx.Commit()
}
