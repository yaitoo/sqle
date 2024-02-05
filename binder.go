package sqle

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"
)

var (
	binders   = make(map[reflect.Type]Binder)
	bindersMu sync.RWMutex

	columns   = make(map[string][]string)
	columnsMu sync.RWMutex

	binderType = reflect.TypeOf((*Binder)(nil)).Elem()
)

type Binder interface {
	Bind(v reflect.Value, columns []string) []any
}

func getColumns(query string, rows *sql.Rows) ([]string, error) {
	columnsMu.RLock()

	var cols []string
	var cached bool

	defer func() {
		columnsMu.RUnlock()

		if !cached {
			columnsMu.Lock()
			columns[query] = cols
			columnsMu.Unlock()
		}
	}()

	cols, cached = columns[query]
	if cached {
		return cols, nil
	}

	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	for _, it := range columns {
		cols = append(cols, strings.ToLower(strings.Replace(it, "_", "", -1)))
	}

	return cols, nil
}
