package sqle

import (
	"database/sql"
	"reflect"
	"strings"
)

var (
	binders = newLRUCache[reflect.Type, Binder](256)
	columns = newLRUCache[string, []string](4096)
)

type Binder interface {
	Bind(v reflect.Value, columns []string) []any
}

func getColumns(query string, rows *sql.Rows) ([]string, error) {
	if cols, ok := columns.Get(query); ok {
		return cols, nil
	}

	rawCols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, len(rawCols))
	for _, it := range rawCols {
		cols = append(cols, strings.ToLower(strings.ReplaceAll(it, "_", "")))
	}

	columns.Put(query, cols)
	return cols, nil
}