package sqle

import (
	"context"
	"database/sql"
)

// RawTx wraps a Tx interface with a per-transaction prepared
// statement cache. It is the concrete type returned by Client.BeginTx
// and Client.Transaction so callers can use the cached prepared
// statements via the Query/Exec/QueryBuilder/ExecBuilder methods.
//
// RawTx's own Query/Exec/Commit/Rollback methods take precedence
// over the embedded Tx interface methods: its Query*/Exec* methods
// return sqle's *Rows/*Row (which add binding), and its Commit/Rollback
// also close the prepared statement cache. The embedded Tx is still
// reachable as the `Tx` field for raw access (e.g. tx.Tx.QueryContext).
//
// Note: RawTx is *not* a Tx itself because its Query*/Exec* method
// signatures differ from the Tx interface (sqle's *Rows/*Row vs. the
// standard library's *sql.Rows/*sql.Row).
type RawTx struct {
	Tx // embedded Tx interface (the underlying transaction)
	noCopy
	stmts map[string]*sql.Stmt
}

func (tx *RawTx) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
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

func (tx *RawTx) closeStmts() {
	for _, stmt := range tx.stmts {
		stmt.Close()
	}
}

func (tx *RawTx) Query(query string, args ...any) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *RawTx) QueryBuilder(ctx context.Context, b *Builder) (*Rows, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	return tx.QueryContext(ctx, query, args...)
}

func (tx *RawTx) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {

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

func (tx *RawTx) QueryRow(query string, args ...any) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *RawTx) QueryRowBuilder(ctx context.Context, b *Builder) *Row {
	query, args, err := b.Build()
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return tx.QueryRowContext(ctx, query, args...)
}

func (tx *RawTx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {

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

func (tx *RawTx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *RawTx) ExecBuilder(ctx context.Context, b *Builder) (sql.Result, error) {
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, query, args...)
}

func (tx *RawTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {

	if len(args) > 0 {
		stmt, err := tx.prepareStmt(ctx, query)
		if err != nil {
			return nil, err
		}

		return stmt.ExecContext(ctx, args...)
	}

	return tx.Tx.ExecContext(context.Background(), query, args...)
}

func (tx *RawTx) Rollback() error {
	defer tx.closeStmts()
	return tx.Tx.Rollback()
}

func (tx *RawTx) Commit() error {
	defer tx.closeStmts()
	return tx.Tx.Commit()
}
