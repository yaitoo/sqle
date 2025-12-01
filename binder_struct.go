package sqle

import (
	"reflect"
	"strings"

	"github.com/iancoleman/strcase"
)

type structBinder struct {
	fieldIndexes     map[string]int
	fieldColumnNames []string
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

func (b *structBinder) Bind(v reflect.Value, columns []string) []any {
	values := make([]any, len(columns))
	var missed any

	for k, n := range columns {
		i, ok := b.fieldIndexes[n]
		if ok {
			values[k] = v.Field(i).Addr().Interface()
		} else {
			values[k] = &missed
		}
	}
	return values
}

func getStructBinder(t reflect.Type, v reflect.Value) Binder {
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

	b = newStructBinder(t, v)
	return b
}
