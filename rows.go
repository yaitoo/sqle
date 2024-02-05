package sqle

import (
	"database/sql"
	"reflect"
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

	list := v.Elem()

	if list.Kind() != reflect.Slice {
		return ErrMustSlice
	}

	var err error

	cols, err := getColumns(r.query, r.Rows)
	if err != nil {
		return err
	}

	listType := list.Type()              //list type
	itemType := listType.Elem()          //item type
	item := reflect.New(itemType).Elem() //item value

	switch itemType.Kind() {
	case reflect.Slice: //[][]T
		list, err = scanToList(v, itemType, list, cols, r.Rows)
		if err != nil {
			return err
		}

	case reflect.Struct: //[]T
		list, err = scanToStructList(item, itemType, list, cols, r.Rows)
		if err != nil {
			return err
		}

	case reflect.Map: //[]map[string]T
		list, err = scanToMapList(item, itemType, list, cols, r.Rows)
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
	return r.Rows.Close()
}
