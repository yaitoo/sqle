package sqle

import "strings"

// WhereBuilder is a struct that represents a SQL WHERE clause builder.
type WhereBuilder struct {
	*Builder
	written    bool
	shouldSkip bool
}

// NewWhere creates a new instance of WhereBuilder.
func NewWhere() *WhereBuilder {
	return &WhereBuilder{
		Builder: New(),
	}
}

// Where adds a WHERE clause to the SQL statement.
// It takes one or more criteria strings as arguments.
// Each criteria string represents a condition in the WHERE clause.
// If a criteria string is empty, it will be ignored.
// Returns a *WhereBuilder that can be used to further build the SQL statement.
func (b *Builder) Where(criteria ...string) *WhereBuilder {
	wb := &WhereBuilder{Builder: b}

	b.stmt.WriteString(" WHERE")
	for _, it := range criteria {
		if it != "" {
			wb.written = true
			b.stmt.WriteString(" ")
			b.stmt.WriteString(it)
		}
	}

	return wb
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

// If sets a condition to skip the subsequent SQL statements.
// If the predicate is false, the subsequent SQL statements will be skipped.
func (wb *WhereBuilder) If(predicate bool) *WhereBuilder {
	wb.shouldSkip = !predicate
	return wb
}

// And adds an AND condition to the WHERE clause.
func (wb *WhereBuilder) And(criteria string) *WhereBuilder {
	return wb.SQL("AND", criteria)
}

// Or adds an OR condition to the WHERE clause.
func (wb *WhereBuilder) Or(criteria string) *WhereBuilder {
	return wb.SQL("OR", criteria)
}

// SQL adds a condition to the WHERE clause with the specified operator.
func (wb *WhereBuilder) SQL(op string, criteria string) *WhereBuilder {
	if wb.shouldSkip {
		wb.shouldSkip = false
		return wb
	}

	if criteria != "" {
		// first condition, op expression should not be written
		if wb.written {
			wb.Builder.stmt.WriteString(" ")
			wb.Builder.stmt.WriteString(op)
		}

		wb.written = true
		wb.Builder.stmt.WriteString(" ")
		wb.Builder.stmt.WriteString(criteria)
	}

	return wb
}

// End returns the underlying Builder instance.
func (wb *WhereBuilder) End() *Builder {
	return wb.Builder
}
