package sqle

import (
	"strings"
)

// OrderByBuilder represents a SQL ORDER BY clause builder.
// It is used to construct ORDER BY clauses for SQL queries.
type OrderByBuilder struct {
	*Builder                 // The underlying SQL query builder.
	written  bool            // Indicates if the ORDER BY clause has been written.
	options  *BuilderOptions // The list of allowed columns for ordering.
}

// NewOrderBy creates a new instance of the OrderByBuilder.
// It takes a variadic parameter `allowedColumns` which specifies the columns that are allowed to be used in the ORDER BY clause.
func NewOrderBy(opts ...BuilderOption) *OrderByBuilder {
	ob := &OrderByBuilder{
		Builder: New(),
		options: &BuilderOptions{},
	}

	for _, o := range opts {
		o(ob.options)
	}

	return ob
}

// WithOrderBy sets the order by clause for the SQL query.
// It takes an instance of the OrderByBuilder and adds the allowed columns to the Builder's order list.
// It also appends the SQL string representation of the OrderByBuilder to the Builder's SQL string.
// It returns a new instance of the OrderByBuilder.
func (b *Builder) WithOrderBy(ob *OrderByBuilder) *OrderByBuilder {
	if ob == nil {
		return nil
	}

	n := b.Order()

	b.SQL(ob.String())

	return n
}

// Order create an OrderByBuilder with allowed columns to prevent sql injection. NB: any input is allowed if it is not provided
func (b *Builder) Order(opts ...BuilderOption) *OrderByBuilder {
	ob := &OrderByBuilder{
		Builder: b,
		options: &BuilderOptions{},
	}

	for _, o := range opts {
		o(ob.options)
	}

	return ob
}

// isAllowed check if column is included in allowed columns. It will remove any untrust input from client
func (ob *OrderByBuilder) getColumn(col string) (string, bool) {
	if ob.options.Columns == nil {
		return col, true
	}

	if ob.options.ToName != nil {
		col = ob.options.ToName(col)
	}

	for _, c := range ob.options.Columns {
		if strings.EqualFold(c, col) {
			return c, true
		}
	}

	return "", false

}

// By order by raw sql. eg By("a asc, b desc")
func (ob *OrderByBuilder) By(raw string) *OrderByBuilder {
	cols := strings.Split(raw, ",")

	var n int
	var items []string
	var by string
	for _, col := range cols {
		items = strings.Split(strings.TrimSpace(col), " ")
		n = len(items)
		switch n {
		case 1:
			ob.ByAsc(strings.TrimSpace(col))
		case 2:
			by = strings.TrimSpace(items[1])
			if strings.EqualFold(by, "ASC") {
				ob.ByAsc(strings.TrimSpace(items[0]))
			} else if strings.EqualFold(by, "DESC") {
				ob.ByDesc(strings.TrimSpace(items[0]))
			}
		}
	}

	return ob

}

// ByAsc order by ascending with columns
func (ob *OrderByBuilder) ByAsc(columns ...string) *OrderByBuilder {
	for _, c := range columns {
		ob.add(c, " ASC")
	}
	return ob
}

// ByDesc order by descending with columns
func (ob *OrderByBuilder) ByDesc(columns ...string) *OrderByBuilder {
	for _, c := range columns {
		ob.add(c, " DESC")
	}
	return ob
}

// add adds a column and its sorting direction to the OrderByBuilder.
// It checks if the column is allowed and appends it to the SQL query.
// If the column has already been written, it appends a comma before adding the column.
// If it's the first column being added, it appends "ORDER BY" before adding the column.
func (ob *OrderByBuilder) add(col, direction string) {
	c, ok := ob.getColumn(col)

	if ok {
		if ob.written {

			ob.Builder.SQL(", ").SQL(ob.quoteColumn(c)).SQL(direction)
		} else {
			// only write once
			if !ob.written {
				ob.Builder.SQL(" ORDER BY ")
			}

			ob.Builder.SQL(ob.quoteColumn(c)).SQL(direction)

			ob.written = true
		}
	}
}
