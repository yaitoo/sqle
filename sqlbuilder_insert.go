package sqle

import (
	"reflect"
)

type InsertBuilder struct {
	b          *Builder
	columns    []string
	values     map[string]any
	table      string
	shouldSkip bool
}

func (ib *InsertBuilder) If(predicate bool) *InsertBuilder {
	ib.shouldSkip = !predicate
	return ib
}

func (ib *InsertBuilder) Set(name string, value any) *InsertBuilder {
	if name == "" {
		return ib
	}
	if ib.values == nil {
		ib.values = make(map[string]any)
	}

	if ib.shouldSkip {
		ib.shouldSkip = false
		return ib
	}

	ib.columns = append(ib.columns, name)
	ib.values[name] = value

	return ib
}

func (ib *InsertBuilder) SetModel(m any) *InsertBuilder {
	if ib.shouldSkip {
		ib.shouldSkip = false
		return ib
	}

	v := reflect.Indirect(reflect.ValueOf(m))
	if v.Kind() != reflect.Struct {
		panic(ErrMustStruct)
	}

	b := getStructBinder(v.Type(), v).(*structBinder)
	for i, n := range b.fieldColumnNames {
		ib.Set(n, v.Field(i).Interface())
	}

	return ib
}

func (ib *InsertBuilder) SetMap(m map[string]any, opts ...BuilderOption) *InsertBuilder {
	if ib.shouldSkip {
		ib.shouldSkip = false
		return ib
	}

	columns := ib.b.sortColumns(m, opts...)

	for _, n := range columns {
		v, ok := m[n]
		if ok {
			ib.Set(n, v)
		}
	}

	return ib
}

func (ib *InsertBuilder) End() *Builder {
	ib.b.SQL("INSERT INTO ").SQL(ib.b.Quote).SQL(ib.table).SQL(ib.b.Quote)

	cols := " ("
	values := " VALUES ("

	for i, n := range ib.columns {
		v := ib.values[n]
		if i == 0 {
			cols += ib.b.Quote + n + ib.b.Quote
			values += "{" + n + "}"
		} else {
			cols += ", " + ib.b.Quote + n + ib.b.Quote
			values += ", {" + n + "}"
		}

		ib.b.Param(n, v)
	}

	ib.b.SQL(cols).SQL(")")
	ib.b.SQL(values).SQL(")")
	ib.b.Params(ib.values)

	return ib.b
}
