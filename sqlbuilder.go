// Package sqle provides a SQLBuilder for constructing SQL statements in a programmatic way.
// It allows you to build SELECT, INSERT, UPDATE, and DELETE statements with ease.
package sqle

import (
	"errors"
	"sort"
	"strings"

	"github.com/yaitoo/sqle/shardid"
)

var (
	// ErrInvalidParamVariable is an error that is returned when an invalid parameter variable is encountered.
	ErrInvalidParamVariable = errors.New("sqle: invalid param variable")

	// DefaultSQLQuote is the default character used to escape column names in UPDATE and INSERT statements.
	DefaultSQLQuote = "`"

	// DefaultSQLParameterize is the default function used to parameterize values in SQL statements.
	DefaultSQLParameterize = func(name string, index int) string {
		return "?"
	}
)

// Builder is a SQL query builder that allows you to construct SQL statements.
type Builder struct {
	stmt       strings.Builder
	inputs     map[string]string
	params     map[string]any
	shouldSkip bool

	Quote        string // escape column name in UPDATE and INSERT
	Parameterize func(name string, index int) string
}

// New creates a new instance of the Builder with the given initial command(s).
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

// Input sets the value of an input variable in the Builder.
func (b *Builder) Input(name, value string) *Builder {
	b.inputs[name] = value
	return b
}

// Inputs sets multiple input variables in the Builder.
func (b *Builder) Inputs(v map[string]string) *Builder {
	for n, v := range v {
		b.Input(n, v)
	}

	return b
}

// Param sets the value of a parameter variable in the Builder.
func (b *Builder) Param(name string, value any) *Builder {
	b.params[name] = value
	return b
}

// Params sets multiple parameter variables in the Builder.
func (b *Builder) Params(v map[string]any) *Builder {
	for n, v := range v {
		b.Param(n, v)
	}

	return b
}

// If sets a condition that determines whether the subsequent SQL command should be executed.
// If the predicate is false, the command is skipped.
func (b *Builder) If(predicate bool) *Builder {
	b.shouldSkip = !predicate
	return b
}

// SQL appends the given SQL command to the Builder's statement.
// If the Builder's shouldSkip flag is set, the command is skipped.
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

// String returns the SQL statement constructed by the Builder.
func (b *Builder) String() string {
	return b.stmt.String()
}

// Build constructs the final SQL statement and returns it along with the parameter values.
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

// WithWhere adds the input and parameter values from the given WhereBuilder to the current Builder
// and sets the WHERE clause of the SQL statement to the string representation of the WhereBuilder's statement.
// It returns the modified WhereBuilder.
func (b *Builder) WithWhere(wb *WhereBuilder) *WhereBuilder {
	for k, v := range wb.inputs {
		b.Input(k, v)
	}

	for k, v := range wb.params {
		b.Param(k, v)
	}

	return b.Where(strings.TrimSpace(wb.stmt.String()))
}

// Where starts a new WhereBuilder and adds the given conditions to the current query builder.
// Returns the new WhereBuilder.
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

// quoteColumn escapes the given column name using the Builder's Quote character.
func (b *Builder) quoteColumn(c string) string {
	if strings.ContainsAny(c, "(") || strings.ContainsAny(c, " ") || strings.ContainsAny(c, "as") {
		return c
	} else {
		return b.Quote + c + b.Quote
	}
}

// Update starts a new UpdateBuilder and sets the table to update.
// Returns the new UpdateBuilder.
func (b *Builder) Update(table string) *UpdateBuilder {
	b.SQL("UPDATE ").SQL(b.Quote).SQL(table).SQL(b.Quote).SQL(" SET ")
	return &UpdateBuilder{
		Builder: b,
	}
}

// Insert starts a new InsertBuilder and sets the table to insert into.
// Returns the new InsertBuilder.
func (b *Builder) Insert(table string) *InsertBuilder {
	return &InsertBuilder{
		b:      b,
		table:  table,
		values: make(map[string]any),
	}
}

// Select adds a SELECT statement to the current query builder.
// If no columns are specified, it selects all columns using "*".
// Returns the current query builder.
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

// Delete adds a DELETE statement to the current query builder.
// Returns the current query builder.
func (b *Builder) Delete(table string) *Builder {
	b.SQL("DELETE FROM ").SQL(b.Quote).SQL(table).SQL(b.Quote)

	return b
}

// On sets the "rotate" input variable to the given shard ID's rotate name.
// Returns the current query builder.
func (b *Builder) On(id shardid.ID) *Builder {
	return b.Input("rotate", id.RotateName())
}

// sortColumns sorts the columns in the given map and returns them as a slice.
// It also allows customization of column names using BuilderOptions.
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
