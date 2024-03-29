package sqle

import (
	"reflect"
)

type UpdateBuilder struct {
	*Builder
	written    bool
	shouldSkip bool
}

func (ub *UpdateBuilder) If(predicate bool) *UpdateBuilder {

	ub.shouldSkip = !predicate

	return ub
}

func (ub *UpdateBuilder) SetModel(dest any) *UpdateBuilder {

	if ub.shouldSkip {
		ub.shouldSkip = false
		return ub
	}

	v := reflect.Indirect(reflect.ValueOf(dest))
	if v.Kind() != reflect.Struct {
		panic(ErrMustStruct)
	}

	b := getStructBinder(v.Type(), v).(*structBinder)
	for i, n := range b.fieldColumnNames {
		ub.Set(n, v.Field(i).Interface())
	}

	return ub

}

func (ub *UpdateBuilder) SetMap(m map[string]any, opts ...BuilderOption) *UpdateBuilder {
	if ub.shouldSkip {
		ub.shouldSkip = false
		return ub
	}

	columns := sortColumns(m, opts...)

	for _, n := range columns {
		v, ok := m[n]
		if ok {
			ub.Set(n, v)
		}
	}

	return ub
}

func (ub *UpdateBuilder) SetExpr(cmd string) *UpdateBuilder {
	if ub.shouldSkip {
		ub.shouldSkip = false
		return ub
	}

	if cmd == "" {
		return ub
	}

	if ub.written {
		ub.SQL(", ")
	}

	ub.written = true
	ub.SQL(cmd)

	return ub
}

func (ub *UpdateBuilder) Set(name string, value any) *UpdateBuilder {
	if ub.shouldSkip {
		ub.shouldSkip = false
		return ub
	}

	if name == "" {
		return ub
	}

	if ub.written {
		ub.SQL(", ")
	}

	ub.written = true
	ub.SQL(ub.Builder.Quote).SQL(name).SQL(ub.Quote)
	ub.SQL("={" + name + "}")
	ub.Param(name, value)

	return ub
}
