package sqle

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

// If sets a condition to skip the subsequent SQL statements.
// If the predicate is false, the subsequent SQL statements will be skipped.
func (wb *WhereBuilder) If(predicate bool) *WhereBuilder {
	wb.shouldSkip = !predicate
	return wb
}

// And appends an AND operator and a command to the SQL statement.
func (wb *WhereBuilder) And(cmd string) *WhereBuilder {
	return wb.Write("AND", cmd)
}

// Or appends an OR operator and a command to the SQL statement.
func (wb *WhereBuilder) Or(cmd string) *WhereBuilder {
	return wb.Write("OR", cmd)
}

// Write appends an operator and a command to the Write statement.
// The operator is only appended if the command is not empty.
func (wb *WhereBuilder) Write(op string, cmd string) *WhereBuilder {
	if wb.shouldSkip {
		wb.shouldSkip = false
		return wb
	}

	if cmd != "" {
		// first condition, op expression should not be written
		if wb.written {
			wb.Builder.stmt.WriteString(" ")
			wb.Builder.stmt.WriteString(op)
		}

		wb.written = true
		wb.Builder.stmt.WriteString(" ")
		wb.Builder.stmt.WriteString(cmd)
	}

	return wb
}

// End returns the underlying Builder instance.
func (wb *WhereBuilder) End() *Builder {
	return wb.Builder
}
