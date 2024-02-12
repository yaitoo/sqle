package sqle

import (
	"context"
	"database/sql"

	"github.com/rs/zerolog/log"
)

type DB struct {
	noCopy
	*sql.DB
}

func Open(db *sql.DB) *DB {
	return &DB{
		DB: db,
	}
}

func (db *DB) Query(query string, args ...any) (*Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryBuilder(ctx context.Context, b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	var rows *sql.Rows
	var stmt *sql.Stmt
	var err error
	if len(args) > 0 {
		stmt, err = prepareStmt(ctx, db.DB, query)
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

func (db *DB) QueryRow(query string, args ...any) *Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowBuilder(ctx context.Context, b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err:   err,
			query: query,
		}
	}

	return db.QueryRowContext(ctx, query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	var rows *sql.Rows
	var stmt *sql.Stmt
	var err error

	if len(args) > 0 {
		stmt, err = prepareStmt(ctx, db.DB, query)
		if err == nil {
			rows, err = stmt.QueryContext(ctx, args...)
		}

	} else {
		rows, err = db.DB.QueryContext(ctx, query, args...)
	}

	return &Row{
		rows:  rows,
		err:   err,
		query: query,
	}
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if len(args) > 0 {
		stmt, err := prepareStmt(ctx, db.DB, query)
		if err != nil {
			return nil, err
		}

		return stmt.ExecContext(ctx, args...)
	}
	return db.DB.ExecContext(context.Background(), query, args...)
}

func (db *DB) Begin(opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx}, nil
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{Tx: tx}, nil
}

func (db *DB) Transaction(ctx context.Context, opts *sql.TxOptions, fn func(tx *Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		if e := tx.Rollback(); e != nil {
			log.Error().Str("pkg", "sqle").Str("tag", "tx").Err(e)
		}
		return err
	}
	return tx.Commit()
}
