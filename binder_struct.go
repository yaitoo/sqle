package sqle

import (
	"reflect"
	"strings"
	"sync"

	"github.com/iancoleman/strcase"
)

type structBinder struct {
	fieldIndexes     map[string]int
	fieldColumnNames []string
	pool             sync.Pool // *[]any — pooled scratch slice for Bind
}

func newStructBinder(t reflect.Type, v reflect.Value) Binder {
	sb := &structBinder{
		fieldIndexes: make(map[string]int),
	}

	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		tagName := f.Tag.Get("db")
		if tagName == "-" {
			continue
		}

		if tagName != "" {
			// Normalize tag name to match column name processing: remove underscores and lowercase
			normalizedTag := strings.ToLower(strings.ReplaceAll(tagName, "_", ""))
			sb.fieldIndexes[normalizedTag] = i
			sb.fieldColumnNames = append(sb.fieldColumnNames, tagName)
			continue

		}

		sb.fieldIndexes[strings.ToLower(f.Name)] = i
		sb.fieldColumnNames = append(sb.fieldColumnNames, strcase.ToSnake(f.Name))
	}

	return sb
}

// acquire returns a []any of length len(columns) and a release func.
// The pointers in the returned slice are obtained via Addr() on the
// fields of v (a freshly allocated struct per row in scanToStructList),
// so the caller MUST call release only after rows.Scan and any consumer
// has finished with the values.
func (b *structBinder) acquire(v reflect.Value, columns []string) ([]any, func()) {
	n := len(columns)
	slotAny := b.pool.Get()
	var slot *[]any
	if slotAny == nil {
		s := make([]any, n)
		slot = &s
	} else {
		slot = slotAny.(*[]any)
	}
	if cap(*slot) < n {
		*slot = make([]any, n)
	} else {
		*slot = (*slot)[:n]
	}

	var missed any
	s := *slot
	for k, name := range columns {
		i, ok := b.fieldIndexes[name]
		if ok {
			s[k] = v.Field(i).Addr().Interface()
		} else {
			s[k] = &missed
		}
	}
	return s, func() {
		// We don't need to clear the slots because the next acquire
		// overwrites every element before Scan runs. We do need to
		// truncate so the pool doesn't grow indefinitely.
		*slot = (*slot)[:0]
		b.pool.Put(slot)
	}
}

func (b *structBinder) Bind(v reflect.Value, columns []string) []any {
	values := make([]any, len(columns))
	var missed any
	for k, name := range columns {
		i, ok := b.fieldIndexes[name]
		if ok {
			values[k] = v.Field(i).Addr().Interface()
		} else {
			values[k] = &missed
		}
	}
	return values
}

func getStructBinder(t reflect.Type, v reflect.Value) Binder {
	if b, ok := binders.Get(t); ok {
		return b
	}

	b := newStructBinder(t, v)
	binders.Put(t, b)
	return b
}