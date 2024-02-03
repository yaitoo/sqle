package sqle

import (
	"errors"
	"strings"
)

var (
	ErrInvalidTextVariable  = errors.New("sqle: invalid text variable")
	ErrInvalidParamVariable = errors.New("sqle: invalid param variable")
)

type Builder struct {
	stmt   strings.Builder
	inputs map[string]string
	params map[string]any

	Quote string //escape column name in UPDATE and INSERT

}

func New(cmd ...string) *Builder {

	b := &Builder{
		inputs: make(map[string]string),
		params: make(map[string]any),

		Quote: "`",
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

func (b *Builder) SQL(cmd string) *Builder {
	if cmd == "" {
		return b
	}
	b.stmt.WriteString(cmd)
	return b
}

func (b *Builder) String() string {
	return b.stmt.String()
}

func (b *Builder) Build() (string, []any, error) {
	tz := Tokenize(b.stmt.String())

	var params []any
	var sb strings.Builder

	for _, t := range tz.Tokens {
		switch t.Type() {
		case TextToken:
			sb.WriteString(t.String())
		case InputToken:
			n := t.String()
			v, ok := b.inputs[n]
			if !ok {
				return "", nil, ErrInvalidTextVariable
			}
			sb.WriteString(v)

		case ParamToken:
			n := t.String()
			v, ok := b.params[n]
			if !ok {
				return "", nil, ErrInvalidParamVariable
			}

			sb.WriteString("?")
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

func (b *Builder) Update(table string) *UpdateBuilder {
	b.SQL("UPDATE ").SQL(table).SQL(" SET ")
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
