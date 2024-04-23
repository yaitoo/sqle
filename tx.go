package sqle

import (
	"context"
	"database/sql"
)

type Tx struct {
	*sql.Tx
	noCopy //nolint
	stmts  map[string]*sql.Stmt
}

func (tx *Tx) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if tx.stmts == nil {
		tx.stmts = make(map[string]*sql.Stmt)
	}
	s, ok := tx.stmts[query]
	if ok {
		return s, nil
	}

	s, err := tx.Tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	tx.stmts[query] = s

	return s, nil
}

func (tx *Tx) closeStmts() {
	for _, stmt := range tx.stmts {
		stmt.Close()
	}
}

func (tx *Tx) Query(query string, args ...any) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryBuilder(ctx context.Context, b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {

	if len(args) > 0 {
		stmt, err := tx.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		rows, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return nil, err
		}
		return &Rows{Rows: rows, query: query}, nil
	}

	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{Rows: rows, query: query}, nil
}

func (tx *Tx) QueryRow(query string, args ...any) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowBuilder(ctx context.Context, b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return tx.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {

	if len(args) > 0 {
		stmt, err := tx.prepareStmt(ctx, query)
		if err != nil {
			return &Row{
				err:   err,
				query: query,
			}
		}

		rows, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return &Row{
				err:   err,
				query: query,
			}
		}
		return &Row{
			rows:  rows,
			query: query,
		}
	}

	rows, err := tx.Tx.QueryContext(ctx, query, args...)

	return &Row{
		rows:  rows,
		err:   err,
		query: query,
	}
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {

	if len(args) > 0 {
		stmt, err := tx.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		return stmt.ExecContext(ctx, args...)
	}

	return tx.Tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) Rollback() error {
	defer tx.closeStmts()
	return tx.Tx.Rollback()
}

func (tx *Tx) Commit() error {
	defer tx.closeStmts()
	return tx.Tx.Commit()
}
