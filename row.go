package sqle

import (
	"database/sql"
	"errors"
	"reflect"
	"time"
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
	rows  *sql.Rows
	err   error
	query string
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()
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
	return r.rows.Close()
}

func (r *Row) Err() error {
	return r.err
}

func (r *Row) Bind(dest any) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Pointer {
		return ErrMustPointer
	}

	if v.IsNil() {
		return ErrMustNotNilPointer
	}

	var err error

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
	return r.rows.Close()
}

func scanTo(dest any, destValue reflect.Value, cols []string, rows *sql.Rows) (bool, error) {
	var err error
	switch b := dest.(type) {
	case *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *[]byte,
		*uintptr, *float32, *float64, *bool, *string, *time.Time,
		sql.Scanner:
		err = rows.Scan(dest)
		if err != nil {
			return true, err
		}

		return true, rows.Close()
	case Binder:
		err = rows.Scan(b.Bind(destValue, cols)...)
		if err != nil {
			return true, err
		}
		return true, rows.Close()
	}

	return false, nil
}

func scanToStruct(v reflect.Value, cols []string, rows *sql.Rows) error {
	b := getStructBinder(v.Type(), v)
	return rows.Scan(b.Bind(v, cols)...)
}
