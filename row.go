package sqle

import (
	"database/sql"
	"errors"
	"reflect"
)

var (
	ErrMustPointer       = errors.New("sqle: dest must be a pointer")
	ErrMustSlice         = errors.New("sqle: dest must be a slice")
	ErrMustStruct        = errors.New("sqle: dest must be a struct")
	ErrMustNotNilPointer = errors.New("sqle: dest must be not a nil pointer")
	ErrTypeNotBindable   = errors.New("sqle: dest type is not bindable")
	ErrMustStringKey     = errors.New("sqle: map key must be string type")
)

type Row struct {
	rows *sql.Rows

	stmt   *Stmt
	err    error
	query  string
	closed bool
}

func (r *Row) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true

	var err error
	// Close the underlying *sql.Rows *before* releasing the Stmt ref. This
	// closes the race window where closeStaleStmt could observe
	// !isUsing on the Stmt while a *sql.Rows is still open, which would
	// surface as "sql: statement is closed" on later rows.Next / rows.Scan.
	// We intentionally do *not* nil r.rows: a subsequent Bind/Scan on the
	// same wrapper must still observe a non-nil *sql.Rows whose Next()
	// returns false (driver's "Rows are closed" state) instead of
	// dereferencing nil.
	if r.rows != nil {
		err = r.rows.Close()
	}

	if r.stmt != nil {
		r.stmt.Reuse()
		r.stmt = nil
	}

	return err
}

func (r *Row) Scan(dest ...any) error {
	defer r.Close()

	if r.err != nil {
		return r.err
	}

	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	// The deferred Row.Close sees r.closed == false on its first entry,
	// does its own *sql.Rows.Close (idempotent at the driver level for
	// sqlite3), and releases the Stmt ref. The r.rows reference is kept
	// intact so a later Scan/Bind can detect the closed-cursor state
	// via r.rows.Next() instead of dereferencing nil.
	return r.rows.Close()
}

func (r *Row) Err() error {
	return r.err
}

func (r *Row) Bind(dest any) error {
	defer r.Close()

	if r.err != nil {
		return r.err
	}

	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Pointer {
		return ErrMustPointer
	}

	if v.IsNil() {
		return ErrMustNotNilPointer
	}

	var err error
	if !r.rows.Next() {
		err = r.rows.Err()
		if err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	cols, err := getColumns(r.query, r.rows)
	if err != nil {
		return err
	}

	ok, err := scanTo(dest, v, cols, r.rows)
	if ok {
		return err
	}

	v = v.Elem()

	switch v.Kind() {
	case reflect.Struct:
		err = scanToStruct(v, cols, r.rows)
		if err != nil {
			return err
		}

	case reflect.Map:
		err = scanToMap(v, cols, r.rows)
		if err != nil {
			return err
		}

	default:
		return ErrTypeNotBindable
	}

	// Make sure the query can be processed to completion with no errors.
	// The deferred Row.Close sees r.closed == false on its first entry,
	// does its own *sql.Rows.Close (idempotent at the driver level for
	// sqlite3), and releases the Stmt ref. The r.rows reference is kept
	// intact so a later Scan/Bind can detect the closed-cursor state
	// via r.rows.Next() instead of dereferencing nil.
	return r.rows.Close()
}
