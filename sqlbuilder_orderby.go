package sqle

import (
	"slices"
	"strings"
)

type OrderByBuilder struct {
	*Builder
	isWritten   bool
	safeColumns []string
}

// OrderBy create an OrderByBuilder with safe columns to prevent sql injection
func (b *Builder) OrderBy(safeColumns ...string) *OrderByBuilder {
	ob := &OrderByBuilder{
		Builder:     b,
		safeColumns: safeColumns,
	}

	b.SQL(" ORDER BY ")

	return ob
}

// isSafe check if column is included in safe columns.
func (ob *OrderByBuilder) isSafe(col string) bool {
	if ob.safeColumns == nil {
		return true
	}

	return slices.ContainsFunc(ob.safeColumns, func(c string) bool {
		return strings.EqualFold(c, col)
	})
}

// Asc order by ASC with columns
func (ob *OrderByBuilder) Asc(columns ...string) *OrderByBuilder {
	for _, c := range columns {
		if ob.isSafe(c) {
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

// Desc order by desc with columns
func (ob *OrderByBuilder) Desc(columns ...string) *OrderByBuilder {
	for _, c := range columns {
		if ob.isSafe(c) {
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
