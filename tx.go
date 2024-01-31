package sqle

import (
	"context"
	"database/sql"
)

type Tx struct {
	*sql.Tx
	noCopy
}

func (tx *Tx) Query(query string, args ...any) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryBuilder(b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return tx.QueryContext(context.TODO(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{Rows: rows, query: query}, nil
}

func (tx *Tx) QueryRow(query string, args ...any) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowBuilder(b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return tx.QueryRowContext(context.TODO(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	rows, err := tx.Tx.QueryContext(ctx, query, args...)

	return &Row{
		rows:  rows,
		err:   err,
		query: query,
	}
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.Tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.Tx.ExecContext(context.Background(), query, args...)
}
