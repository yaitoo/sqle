package sqle

import (
	"slices"
	"strings"
)

type OrderByBuilder struct {
	*Builder
	isWritten      bool
	allowedColumns []string
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

	b.SQL(" ORDER BY ")

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
		if ob.isAllowed(c) {
			if ob.isWritten {
				ob.Builder.SQL(", ").SQL(c).SQL(" ASC")
			} else {
				ob.Builder.SQL(c).SQL(" ASC")
				ob.isWritten = true
			}
		}
	}
	return ob
}

// ByDesc order by descending with columns
func (ob *OrderByBuilder) ByDesc(columns ...string) *OrderByBuilder {
	for _, c := range columns {
		if ob.isAllowed(c) {
			if ob.isWritten {
				ob.Builder.SQL(", ").SQL(c).SQL(" DESC")
			} else {
				ob.Builder.SQL(c).SQL(" DESC")
				ob.isWritten = true
			}
		}
	}
	return ob
}
