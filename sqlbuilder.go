package sqle

import (
	"errors"
	"sort"
	"strings"

	"github.com/yaitoo/sqle/shardid"
)

var (
	ErrInvalidParamVariable = errors.New("sqle: invalid param variable")
)

var (
	DefaultSQLQuote        = "`"
	DefaultSQLParameterize = func(name string, index int) string {
		return "?"
	}
)

type Builder struct {
	stmt       strings.Builder
	inputs     map[string]string
	params     map[string]any
	shouldSkip bool

	Quote        string // escape column name in UPDATE and INSERT
	Parameterize func(name string, index int) string
}

func New(cmd ...string) *Builder {

	b := &Builder{
		inputs:       make(map[string]string),
		params:       make(map[string]any),
		Quote:        DefaultSQLQuote,
		Parameterize: DefaultSQLParameterize,
	}

	for i, it := range cmd {
		if i > 0 {
			b.stmt.WriteString(" ")
		}
		b.stmt.WriteString(it)
	}

	return b
}

func (b *Builder) Input(name, value string) *Builder {
	b.inputs[name] = value
	return b
}

func (b *Builder) Inputs(v map[string]string) *Builder {
	for n, v := range v {
		b.Input(n, v)
	}

	return b
}

func (b *Builder) Param(name string, value any) *Builder {
	b.params[name] = value
	return b
}

func (b *Builder) Params(v map[string]any) *Builder {
	for n, v := range v {
		b.Param(n, v)
	}

	return b
}

func (b *Builder) If(predicate bool) *Builder {
	b.shouldSkip = !predicate
	return b
}

func (b *Builder) SQL(cmd string) *Builder {
	if b.shouldSkip {
		b.shouldSkip = false
		return b
	}

	if cmd != "" {
		b.stmt.WriteString(cmd)
	}
	return b
}

func (b *Builder) String() string {
	return b.stmt.String()
}

func (b *Builder) Build() (string, []any, error) {
	tz := Tokenize(b.stmt.String())

	var params []any
	var sb strings.Builder
	i := 1

	for _, t := range tz.Tokens {
		switch t.Type() {
		case TextToken:
			sb.WriteString(t.String())
		case InputToken:
			n := t.String()
			v, ok := b.inputs[n]
			if ok {
				sb.WriteString(v)
			}

		case ParamToken:
			n := t.String()
			v, ok := b.params[n]
			if !ok {
				return "", nil, ErrInvalidParamVariable
			}

			sb.WriteString(b.Parameterize(n, i))
			i++
			params = append(params, v)
		}

	}

	return sb.String(), params, nil

}

func (b *Builder) Where(cmd ...string) *WhereBuilder {
	wb := &WhereBuilder{Builder: b}

	b.stmt.WriteString(" WHERE")
	for _, it := range cmd {
		if it != "" {
			wb.written = true
			b.stmt.WriteString(" ")
			b.stmt.WriteString(it)
		}
	}

	return wb
}

func (b *Builder) quoteColumn(c string) string {
	if strings.ContainsAny(c, "(") || strings.ContainsAny(c, " ") || strings.ContainsAny(c, "as") {
		return c
	} else {
		return b.Quote + c + b.Quote
	}
}

func (b *Builder) Update(table string) *UpdateBuilder {
	b.SQL("UPDATE ").SQL(b.Quote).SQL(table).SQL(b.Quote).SQL(" SET ")
	return &UpdateBuilder{
		Builder: b,
	}
}

func (b *Builder) Insert(table string) *InsertBuilder {
	return &InsertBuilder{
		b:      b,
		table:  table,
		values: make(map[string]any),
	}
}

func (b *Builder) Select(table string, columns ...string) *Builder {
	b.SQL("SELECT")

	if columns == nil {
		b.SQL(" *")
	} else {
		for i, col := range columns {
			if i == 0 {
				b.SQL(" ").SQL(b.quoteColumn(col))
			} else {
				b.SQL(" ,").SQL(b.quoteColumn(col))
			}
		}
	}

	b.SQL(" FROM ").SQL(b.Quote).SQL(table).SQL(b.Quote)

	return b
}

func (b *Builder) Delete(table string) *Builder {
	b.SQL("DELETE FROM ").SQL(b.Quote).SQL(table).SQL(b.Quote)

	return b
}

func (b *Builder) On(id shardid.ID) *Builder {
	return b.Input("rotate", id.RotateName())
}

func sortColumns(m map[string]any, opts ...BuilderOption) []string {

	bo := &BuilderOptions{}
	for _, opt := range opts {
		opt(bo)
	}

	hasCustomizedColumns := len(bo.Columns) > 0

	for n, v := range m {

		name := n

		if bo.ToName != nil {
			name = bo.ToName(name)
			if name != n {
				m[name] = v
			}

		}

		if !hasCustomizedColumns {
			bo.Columns = append(bo.Columns, name)
		}
	}

	if !hasCustomizedColumns {
		sort.Strings(bo.Columns)
	}

	return bo.Columns

}
