package sqle

type WhereBuilder struct {
	*Builder
	written   bool
	skipTimes int
}

func (wb *WhereBuilder) If(predicate bool) *WhereBuilder {
	if !predicate {
		wb.skipTimes++
	}

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

	if wb.skipTimes > 0 {
		wb.skipTimes--
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
