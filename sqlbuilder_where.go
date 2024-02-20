package sqle

type WhereBuilder struct {
	*Builder
	written    bool
	shouldSkip bool
}

func (wb *WhereBuilder) If(predicate bool) *WhereBuilder {
	wb.shouldSkip = !predicate
	return wb
}

func (wb *WhereBuilder) And(cmd string) *WhereBuilder {
	return wb.SQL("AND", cmd)
}

func (wb *WhereBuilder) Or(cmd string) *WhereBuilder {
	return wb.SQL("OR", cmd)
}

func (wb *WhereBuilder) SQL(op string, cmd string) *WhereBuilder {

	if cmd == "" {
		return wb
	}

	if wb.shouldSkip {
		wb.shouldSkip = false
		return wb
	}

	//first condition, op expression should not be written
	if wb.written {
		wb.Builder.stmt.WriteString(" ")
		wb.Builder.stmt.WriteString(op)
	}

	wb.written = true
	wb.Builder.stmt.WriteString(" ")
	wb.Builder.stmt.WriteString(cmd)

	return wb
}

func (wb *WhereBuilder) End() *Builder {
	return wb.Builder
}
