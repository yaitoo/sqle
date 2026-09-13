package sqle

import (
	"database/sql"
	"reflect"
)

type Rows struct {
	*sql.Rows
	stmt  *Stmt
	query string
}

func (r *Rows) Close() error {
	var err error
	// Close the underlying *sql.Rows *before* releasing the Stmt ref. This
	// closes the race window where closeStaleStmt could observe
	// !isUsing on the Stmt while a *sql.Rows is still open, which would
	// surface as "sql: statement is closed" on later rows.Next / rows.Scan.
	if r.Rows != nil {
		err = r.Rows.Close()
		r.Rows = nil
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
	// Nil r.Rows before returning so the deferred Rows.Close is a no-op
	// rather than a second close of the same *sql.Rows.
	err = r.Rows.Close()
	r.Rows = nil
	return err
}
