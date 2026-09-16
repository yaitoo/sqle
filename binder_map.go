package sqle

import (
	"database/sql"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

type mapBinder struct {
	elem     any
	elemType reflect.Type
	pool     sync.Pool // *mapBinderSlot
}

// mapBinderSlot is a reusable scratch space for a single mapBinder.
// values[i] is a *T pointer into backing at offset i*sz. After the caller
// has consumed the values (typically after rows.Scan and copying them to a
// stable destination), it must invoke the release func returned from
// acquire to return the slot to the binder's pool.
//
// The slot is internal: the public Binder interface is preserved for
// backward compatibility, and the original mapBinder.Bind still allocates
// a fresh []any per call (it does not participate in the pool). The
// internal acquire path used by scan.go is what skips the per-row
// allocations described in issue #78.
type mapBinderSlot struct {
	values  []any
	backing []byte
}

// acquire returns a []any of length n and a release func. Each values[i]
// points into the slot's backing storage; rows.Scan writes through those
// pointers. Callers MUST call release after the values are consumed
// (e.g., after copying them into the destination map/slice).
func (b *mapBinder) acquire(n int) ([]any, func()) {
	slotAny := b.pool.Get()
	if slotAny == nil {
		slotAny = &mapBinderSlot{}
	}
	slot := slotAny.(*mapBinderSlot)

	elemType := b.elemType
	sz := int(elemType.Size())
	need := n * sz

	if cap(slot.values) < n {
		slot.values = make([]any, n)
	} else {
		slot.values = slot.values[:n]
	}
	if cap(slot.backing) < need {
		slot.backing = make([]byte, need)
	}

	base := unsafe.Pointer(&slot.backing[0])
	for i := 0; i < n; i++ {
		addr := unsafe.Add(base, uintptr(i*sz))
		slot.values[i] = reflect.NewAt(elemType, addr).Interface()
	}

	return slot.values, func() {
		// Drop references so any internal pointers held by the
		// previous values (e.g., time.Time *Location, sql.NullString)
		// can be GC'd while the slot sits idle in the pool. The
		// backing bytes are overwritten on next acquire, but we clear
		// them too to be safe with non-trivial element layouts.
		for i := range slot.values {
			slot.values[i] = nil
		}
		for i := range slot.backing {
			slot.backing[i] = 0
		}
		slot.values = slot.values[:0]
		b.pool.Put(slot)
	}
}

// Bind satisfies the public Binder interface. It allocates a fresh slice
// per call so external Binder implementations are not affected; internal
// callers in scan.go use acquire instead and benefit from the pool.
func (b *mapBinder) Bind(_ reflect.Value, columns []string) []any {
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

func getMapBinder(t reflect.Type, _ reflect.Type) Binder {
	if b, ok := binders.Get(t); ok {
		return b
	}

	b := &mapBinder{
		elem:     reflect.New(t.Elem()).Interface(),
		elemType: t.Elem(),
	}

	binders.Put(t, b)
	return b
}