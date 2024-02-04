package sqle

import (
	"database/sql"
	"reflect"
	"time"
)

type Rows struct {
	*sql.Rows
	query string
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

	ev := v.Elem()

	if ev.Kind() != reflect.Slice {
		return ErrMustSlice
	}

	var err error

	cols, err := getColumns(r.query, r.Rows)
	if err != nil {
		return err
	}

	n := len(cols)

	lType := ev.Type()                  //list type
	iType := lType.Elem()               //item type
	iValue := reflect.New(iType).Elem() //item value

	var b Binder

	switch iType.Kind() {
	case reflect.Slice:

		itemType := iType.Elem()
		itemValue := reflect.New(itemType).Interface()

		switch itemValue.(type) {
		case *int, *int8, *int16, *int32, *int64,
			*uint, *uint8, *uint16, *uint32, *uint64,
			*uintptr, *float32, *float64, *bool, *string, *time.Time,
			sql.Scanner:

			for r.Rows.Next() {
				values := make([]any, 0, n)
				for i := 0; i < n; i++ {
					values = append(values, reflect.New(itemType).Interface())
				}

				err = r.Rows.Scan(values...)
				if err != nil {
					return err
				}

				fields := reflect.MakeSlice(iType, 0, n)
				for i := 0; i < n; i++ {
					fields = reflect.Append(fields, reflect.ValueOf(values[i]).Elem())
				}
				ev = reflect.Append(ev, fields)
			}

		default:
			return ErrTypeNotBindable

		}

	case reflect.Struct:
		b = getStructBinder(iValue.Type(), iValue)

		for r.Rows.Next() {
			it := reflect.New(iType)
			err = r.Rows.Scan(b.Bind(it.Elem(), cols)...)
			if err != nil {
				return err
			}

			ev = reflect.Append(ev, it.Elem())
		}

	case reflect.Map:
		vt := iValue.Type()
		kt := vt.Key()
		if kt.Kind() != reflect.String {
			return ErrMustStringKey
		}
		b = getMapBinder(vt, kt)

		fields := b.Bind(ev, cols)

		for r.Rows.Next() {
			err = r.Rows.Scan(fields...)
			if err != nil {
				return err
			}

			it := reflect.MakeMap(iType)
			for i, n := range cols {
				it.SetMapIndex(reflect.ValueOf(n), reflect.ValueOf(fields[i]).Elem())
			}
			ev = reflect.Append(ev, it)
		}

	default:
		return ErrTypeNotBindable
	}

	err = r.Rows.Err()
	if err != nil {
		return err
	}

	v.Elem().Set(ev)

	// Make sure the query can be processed to completion with no errors.
	return r.Rows.Close()

}
