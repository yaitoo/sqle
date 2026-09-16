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
	b := getStructBinder(v.Type(), v).(*structBinder)
	fields, release := b.acquire(v, cols)
	defer release()
	return rows.Scan(fields...)
}

func scanToMap(dest reflect.Value, cols []string, rows *sql.Rows) error {
	vt := dest.Type()
	kt := vt.Key()
	if kt.Kind() != reflect.String {
		return ErrMustStringKey
	}
	b := getMapBinder(vt, kt).(*mapBinder)

	fields, release := b.acquire(len(cols))
	defer release()

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

		// Pre-allocate a scratch slice whose backing storage holds the
		// Scan targets for every row. Each values[i] is a *T pointer into
		// scratch's backing array, so rows.Scan writes directly into the
		// scratch and we avoid allocating `reflect.New(elem)` per column
		// per row (issue #78). scratch is a typed slice so the GC keeps
		// any pointer fields (string data, time.Time *Location,
		// sql.NullString data) alive across iterations.
		scratch := reflect.MakeSlice(itemType, n, n)
		values := make([]any, n)
		populateScanTargets(values, scratch, n)

		for rows.Next() {
			err = rows.Scan(values...)
			if err != nil {
				return list, err
			}

			fields := reflect.MakeSlice(itemType, n, n)
			// reflect.Copy uses typedslicecopy / typedmemmove, which
			// emit the GC write barriers required for pointer-bearing
			// elements (string, time.Time, sql.Scanner, etc.).
			reflect.Copy(fields, scratch)
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

// populateScanTargets fills values[i] with a *T pointer into the i-th slot
// of scratch's backing array. After this call, rows.Scan(values...) writes
// through those pointers into the scratch — no per-row reflect.New(elem).
//
// values must have length n; scratch must be a slice of the same element
// type with length n. scratch is a typed []T so the GC scans pointer
// fields (string data, time.Time *Location, sql.NullString fields)
// written by Scan.
func populateScanTargets(values []any, scratch reflect.Value, n int) {
	if n == 0 {
		return
	}
	for i := 0; i < n; i++ {
		values[i] = scratch.Index(i).Addr().Interface()
	}
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
	b := getStructBinder(item.Type(), item).(*structBinder)

	for rows.Next() {
		it := reflect.New(itemType)
		fields, release := b.acquire(it.Elem(), cols)
		err = rows.Scan(fields...)
		release()
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
	b := getMapBinder(vt, kt).(*mapBinder)

	fields, release := b.acquire(len(cols))
	defer release()
	var err error
	// Pre-size the map so the n SetMapIndex calls don't trigger
	// rehashes. The map itself must be allocated per row (issue #78)
	// because reflect.Append shares the underlying map across all
	// appended entries; clearing and re-using a single map would alias
	// every entry in the resulting list.
	for rows.Next() {
		err = rows.Scan(fields...)
		if err != nil {
			return list, err
		}

		it := reflect.MakeMapWithSize(itemType, len(cols))
		for i, n := range cols {
			it.SetMapIndex(reflect.ValueOf(n), reflect.ValueOf(fields[i]).Elem())
		}
		list = reflect.Append(list, it)
	}
	return list, nil
}
