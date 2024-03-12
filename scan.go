package sqle

import (
	"database/sql"
	"reflect"
	"time"
)

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

func scanToMap(dest reflect.Value, cols []string, rows *sql.Rows) error {
	vt := dest.Type()
	kt := vt.Key()
	if kt.Kind() != reflect.String {
		return ErrMustStringKey
	}
	b := getMapBinder(vt, kt)

	fields := b.Bind(dest, cols)

	err := rows.Scan(fields...)
	if err != nil {
		return err
	}

	for i, n := range cols {
		it := fields[i]
		dest.SetMapIndex(reflect.ValueOf(n), reflect.ValueOf(it).Elem())
	}
	return nil
}

func scanToList(item reflect.Value, itemType reflect.Type, list reflect.Value, cols []string, rows *sql.Rows) (reflect.Value, error) {
	var err error
	n := len(cols)
	elem := itemType.Elem()
	it := reflect.New(elem).Interface()

	switch b := it.(type) {
	case *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *[]byte,
		*uintptr, *float32, *float64, *bool, *string, *time.Time,
		sql.Scanner:

		for rows.Next() {
			values := make([]any, 0, n)
			for i := 0; i < n; i++ {
				values = append(values, reflect.New(elem).Interface())
			}

			err = rows.Scan(values...)
			if err != nil {
				return list, err
			}

			fields := reflect.MakeSlice(itemType, 0, n)
			for i := 0; i < n; i++ {
				fields = reflect.Append(fields, reflect.ValueOf(values[i]).Elem())
			}
			list = reflect.Append(list, fields)
		}
	case Binder:
		for rows.Next() {
			values := b.Bind(item, cols)

			err = rows.Scan(values...)
			if err != nil {
				return list, err
			}

			fields := reflect.MakeSlice(itemType, 0, n)
			for i := 0; i < n; i++ {
				fields = reflect.Append(fields, reflect.ValueOf(values[i]).Elem())
			}
			list = reflect.Append(list, fields)
		}

	default:
		return list, ErrTypeNotBindable
	}
	return list, nil
}

func scanToBinderList(_ reflect.Value, itemType reflect.Type, list reflect.Value, cols []string, rows *sql.Rows) (reflect.Value, error) {

	var err error

	for rows.Next() {
		it := reflect.New(itemType)
		b, _ := it.Interface().(Binder)
		err = rows.Scan(b.Bind(it.Elem(), cols)...)
		if err != nil {
			return reflect.Value{}, err
		}

		list = reflect.Append(list, it.Elem())
	}
	return list, nil
}

func scanToStructList(item reflect.Value, itemType reflect.Type, list reflect.Value, cols []string, rows *sql.Rows) (reflect.Value, error) {

	var err error
	b := getStructBinder(item.Type(), item)

	for rows.Next() {
		it := reflect.New(itemType)
		err = rows.Scan(b.Bind(it.Elem(), cols)...)
		if err != nil {
			return reflect.Value{}, err
		}

		list = reflect.Append(list, it.Elem())
	}
	return list, nil
}

func scanToMapList(item reflect.Value, itemType reflect.Type, list reflect.Value, cols []string, rows *sql.Rows) (reflect.Value, error) {
	vt := item.Type()
	kt := vt.Key()
	if kt.Kind() != reflect.String {
		return list, ErrMustStringKey
	}
	b := getMapBinder(vt, kt)

	fields := b.Bind(list, cols)
	var err error
	for rows.Next() {
		err = rows.Scan(fields...)
		if err != nil {
			return list, err
		}

		it := reflect.MakeMap(itemType)
		for i, n := range cols {
			it.SetMapIndex(reflect.ValueOf(n), reflect.ValueOf(fields[i]).Elem())
		}
		list = reflect.Append(list, it)
	}
	return list, nil
}
