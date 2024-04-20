package sqle

import (
	"slices"
	"strings"
)

// OrderByBuilder represents a SQL ORDER BY clause builder.
// It is used to construct ORDER BY clauses for SQL queries.
type OrderByBuilder struct {
	*Builder                // The underlying SQL query builder.
	written        bool     // Indicates if the ORDER BY clause has been written.
	allowedColumns []string // The list of allowed columns for ordering.
}

// NewOrderBy creates a new instance of the OrderByBuilder.
// It takes a variadic parameter `allowedColumns` which specifies the columns that are allowed to be used in the ORDER BY clause.
func NewOrderBy(allowedColumns ...string) *OrderByBuilder {
	return &OrderByBuilder{
		Builder:        New(),
		allowedColumns: allowedColumns,
	}
}

// WithOrderBy sets the order by clause for the SQL query.
// It takes an instance of the OrderByBuilder and adds the allowed columns to the Builder's order list.
// It also appends the SQL string representation of the OrderByBuilder to the Builder's SQL string.
// It returns a new instance of the OrderByBuilder.
func (b *Builder) WithOrderBy(ob *OrderByBuilder) *OrderByBuilder {
	if ob == nil {
		return nil
	}

	n := b.Order(ob.allowedColumns...)

	b.SQL(ob.String())

	return n
}

// Order create an OrderByBuilder with allowed columns to prevent sql injection. NB: any input is allowed if it is not provided
func (b *Builder) Order(allowedColumns ...string) *OrderByBuilder {
	ob := &OrderByBuilder{
		Builder:        b,
		allowedColumns: allowedColumns,
	}

	return ob
}

// isAllowed check if column is included in allowed columns. It will remove any untrust input from client
func (ob *OrderByBuilder) isAllowed(col string) bool {
	if ob.allowedColumns == nil {
		return true
	}

	return slices.ContainsFunc(ob.allowedColumns, func(c string) bool {
		return strings.EqualFold(c, col)
	})
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
	if ob.isAllowed(col) {
		if ob.written {
			ob.Builder.SQL(", ").SQL(col).SQL(direction)
		} else {
			// only write once
			if !ob.written {
				ob.Builder.SQL(" ORDER BY ")
			}

			ob.Builder.SQL(col).SQL(direction)

			ob.written = true
		}
	}
}
