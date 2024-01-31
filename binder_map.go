package sqle

import (
	"database/sql"
	"reflect"
	"time"
)

type mapBinder struct {
	elem any
}

func (b *mapBinder) Bind(v reflect.Value, columns []string) []any {
	values := make([]any, len(columns))

	switch b.elem.(type) {
	case *int:
		for i := range columns {
			var it int
			values[i] = &it
		}
	case *int8:
		for i := range columns {
			var it int8
			values[i] = &it
		}
	case *int16:
		for i := range columns {
			var it int16
			values[i] = &it
		}
	case *int32:
		for i := range columns {
			var it int32
			values[i] = &it
		}
	case *int64:
		for i := range columns {
			var it int64
			values[i] = &it
		}
	case *uint:
		for i := range columns {
			var it uint
			values[i] = &it
		}
	case *uint8:
		for i := range columns {
			var it uint8
			values[i] = &it
		}
	case *uint16:
		for i := range columns {
			var it uint16
			values[i] = &it
		}
	case *uint32:
		for i := range columns {
			var it uint32
			values[i] = &it
		}
	case *uint64:
		for i := range columns {
			var it uint64
			values[i] = &it
		}
	case *uintptr:
		for i := range columns {
			var it uintptr
			values[i] = &it
		}
	case *float32:
		for i := range columns {
			var it float32
			values[i] = &it
		}
	case *float64:
		for i := range columns {
			var it float64
			values[i] = &it
		}
	case *bool:
		for i := range columns {
			var it bool
			values[i] = &it
		}
	case *string:
		for i := range columns {
			var it string
			values[i] = &it
		}
	case *time.Time:
		for i := range columns {
			var it time.Time
			values[i] = &it
		}
	case *sql.NullInt32:
		for i := range columns {
			it := &sql.NullInt16{}
			values[i] = &it
		}
	case *sql.NullInt64:
		for i := range columns {
			it := &sql.NullInt64{}
			values[i] = &it
		}
	case *sql.NullFloat64:
		for i := range columns {
			it := &sql.NullFloat64{}
			values[i] = &it
		}
	case *sql.NullBool:
		for i := range columns {
			it := &sql.NullBool{}
			values[i] = &it
		}
	case *sql.NullString:
		for i := range columns {
			it := &sql.NullString{}
			values[i] = &it
		}
	case *sql.NullTime:
		for i := range columns {
			it := &sql.NullTime{}
			values[i] = &it
		}
	default:
		for i := range columns {
			var it interface{}
			values[i] = &it
		}
	}

	return values
}

func getMapBinder(t reflect.Type, kt reflect.Type) Binder {
	bindersMu.RLock()
	var b Binder
	var cached bool
	defer func() {
		bindersMu.RUnlock()

		if !cached {
			bindersMu.Lock()
			binders[t] = b
			bindersMu.Unlock()
		}

	}()

	b, cached = binders[t]
	if cached {
		return b
	}

	b = &mapBinder{
		elem: reflect.New(t.Elem()).Interface(),
	}

	return b
}
