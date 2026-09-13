package sqle

import (
	"database/sql"
	"reflect"
)

type Rows struct {
	*sql.Rows
	stmt   *Stmt
	query  string
	closed bool
}

func (r *Rows) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true

	var err error
	// Close the underlying *sql.Rows *before* releasing the Stmt ref. This
	// closes the race window where closeStaleStmt could observe
	// refCount == 0 on the Stmt while a *sql.Rows is still open, which
	// would surface as "sql: statement is closed" on later rows.Next /
	// rows.Scan. We intentionally do *not* nil r.Rows: a subsequent
	// Bind/Scan on the same wrapper must still observe a non-nil
	// *sql.Rows whose Next() returns false (driver's "Rows are closed"
	// state) instead of dereferencing nil.
	if r.Rows != nil {
		err = r.Rows.Close()
	}

	if r.stmt != nil {
		r.stmt.Reuse()
		r.stmt = nil
	}

	return err
}

func (r *Rows) Scan(dest ...any) error {
	return r.Rows.Scan(dest...)
}

func (r *Rows) Bind(dest any) error {
	defer r.Close()

	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Pointer {
		return ErrMustPointer
	}

	if v.IsNil() {
		return ErrMustNotNilPointer
	}

	list := v.Elem()

	if list.Kind() != reflect.Slice {
		return ErrMustSlice
	}

	var err error

	cols, err := getColumns(r.query, r.Rows)
	if err != nil {
		return err
	}

	listType := list.Type()       // list type
	itemType := listType.Elem()   // item type
	item := reflect.New(itemType) // item value

	switch itemType.Kind() {
	case reflect.Slice: // [][]T
		list, err = scanToList(v, itemType, list, cols, r.Rows)
		if err != nil {
			return err
		}

	case reflect.Struct: // []T
		_, ok := item.Interface().(Binder)
		if ok {
			list, err = scanToBinderList(item.Elem(), itemType, list, cols, r.Rows)
			if err != nil {
				return err
			}

		} else {
			list, err = scanToStructList(item.Elem(), itemType, list, cols, r.Rows)
			if err != nil {
				return err
			}
		}

	case reflect.Map: // []map[string]T
		list, err = scanToMapList(item.Elem(), itemType, list, cols, r.Rows)
		if err != nil {
			return err
		}

	default:
		return ErrTypeNotBindable
	}

	err = r.Rows.Err()
	if err != nil {
		return err
	}

	v.Elem().Set(list)

	// Make sure the query can be processed to completion with no errors.
	// The deferred Rows.Close will see r.closed == false on first entry and
	// short-circuit, leaving the *sql.Rows reference intact so a later
	// Bind/Scan can still observe the closed-cursor state via Next().
	return r.Rows.Close()
}
