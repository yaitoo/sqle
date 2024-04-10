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

// OrderBy create an OrderByBuilder with allowed columns to prevent sql injection. NB: any input is allowed if it is not provided
func (b *Builder) OrderBy(allowedColumns ...string) *OrderByBuilder {
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

// Asc order by ASC with columns
func (ob *OrderByBuilder) Asc(columns ...string) *OrderByBuilder {
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

// Desc order by desc with columns
func (ob *OrderByBuilder) Desc(columns ...string) *OrderByBuilder {
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
